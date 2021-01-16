package com.github.martintomac.kafkadelayedproducer

import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.io.Closeable
import java.time.Duration
import java.time.Instant
import java.util.Collections.emptyList
import java.util.Collections.synchronizedList
import java.util.concurrent.*
import java.util.concurrent.TimeUnit.NANOSECONDS
import kotlin.concurrent.thread

class KafkaDelayedProducer<K, V : Any>(
    private val kafkaProducer: Producer<K, V>,
    private val referenceFactory: ReferenceFactory<DelayedRecord<K, V>> = ReferenceFactory.instance(),
    private val errorHandler: ErrorHandler = RetryingErrorHandler(),
    private val clock: Clock = Clock
) : DelayedProducer<K, V>, Closeable {

    companion object {
        private val logger = logger()
    }

    private val delayedRecordReferences = DelayQueue<DelayedRecordReference>()
    private val availableRecordSender = AvailableRecordSender()
    @Volatile
    private var closed = false

    override val numOfUnsentRecords: Int
        get() = delayedRecordReferences.size + availableRecordSender.numOfOutboxRecords

    override fun send(
        record: ProducerRecord<K, V>,
        afterDuration: Duration
    ) {
        val delayedRecord = DelayedRecord(record, afterDuration, clock.now())
        logger.trace { "Delaying $record send until ${delayedRecord.scheduledOnTime}" }
        delayedRecordReferences += DelayedRecordReference(delayedRecord)
    }

    override fun close() {
        if (closed) return
        logger.debug { "Closing delayed producer" }
        closed = true
        availableRecordSender.close()
        kafkaProducer.close()
    }

    private inner class DelayedRecordReference(
        delayedRecord: DelayedRecord<K, V>
    ) : AbstractDelayed(), Reference<ProducerRecord<K, V>> {

        private val reference: Reference<DelayedRecord<K, V>> = referenceFactory.create(delayedRecord)
        private val scheduledOnTime: Instant = delayedRecord.scheduledOnTime

        override val value: ProducerRecord<K, V> get() = reference.value.producerRecord

        override fun getDelay(): Duration = Duration.between(clock.now(), scheduledOnTime)

        override fun release() {
            reference.release()
        }
    }

    private inner class AvailableRecordSender : Closeable {

        private val outboxRecordReferences = synchronizedList(mutableListOf<DelayedRecordReference>())
        val numOfOutboxRecords get() = outboxRecordReferences.size

        private val pollingThread = thread(
            name = "kafka-delayed-producer-thread",
            isDaemon = false
        ) {
            while (!closed) {
                val recordReferences = delayedRecordReferences.poll(1.seconds, 100)
                if (recordReferences.isNotEmpty()) send(recordReferences)
            }
        }

        private fun send(recordReferences: List<DelayedRecordReference>) {
            outboxRecordReferences += recordReferences

            val referenceToFutureList = recordReferences
                .map { reference -> reference to send(reference.value) }

            for ((reference, future) in referenceToFutureList) {
                try {
                    while (!closed) {
                        try {
                            future.get(timeout = 100.millis)
                            break
                        } catch (e: TimeoutException) {
                        }
                    }
                } catch (e: ExecutionException) {
                    val cause = e.cause as Exception
                    handleException(reference.value, cause)
                }

                outboxRecordReferences -= reference
                reference.release()
            }
        }

        private fun send(producerRecord: ProducerRecord<K, V>): Future<RecordMetadata> {
            logger.trace { "Sending record: $producerRecord" }
            return kafkaProducer.send(producerRecord)
        }

        private fun handleException(
            producerRecord: ProducerRecord<K, V>,
            thrownException: Exception
        ) {
            logger.warn(thrownException) { "Failed to send record with exception: $thrownException" }
            logger.debug { "Failed to send record: $producerRecord" }
            while (!closed) {
                try {
                    errorHandler.handle(thrownException, producerRecord, kafkaProducer)
                    logger.debug { "Handled failed send of record: $producerRecord" }
                    return
                } catch (handlingException: Exception) {
                    logger.warn(handlingException) { "Failed to handle record send with exception: $handlingException" }
                }
            }
        }

        override fun close() {
            pollingThread.join()
        }
    }

    private fun <T : Delayed> DelayQueue<T>.poll(
        timeout: Duration,
        limit: Int
    ): List<T> {
        val elements = mutableListOf<T>()

        drainTo(elements, limit)
        if (elements.isNotEmpty()) return elements

        val recordReference = poll(timeout.toNanos(), NANOSECONDS)
            ?: return emptyList()

        elements += recordReference
        if (limit > 1) drainTo(elements, limit - 1)
        return elements
    }

    private fun <T> Future<T>.get(timeout: Duration): T = get(timeout.toNanos(), NANOSECONDS)

    private abstract class AbstractDelayed : Delayed {

        override fun compareTo(other: Delayed): Int {
            return getDelay(NANOSECONDS)
                .compareTo(other.getDelay(NANOSECONDS))
        }

        override fun getDelay(unit: TimeUnit): Long {
            return unit.convert(getDelay().toNanos(), NANOSECONDS)
        }

        abstract fun getDelay(): Duration
    }
}