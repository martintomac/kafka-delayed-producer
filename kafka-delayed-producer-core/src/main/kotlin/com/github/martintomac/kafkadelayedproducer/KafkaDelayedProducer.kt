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

class KafkaDelayedProducer<K, V>(
    private val kafkaProducer: Producer<K, V>,
    private val referenceFactory: ReferenceFactory<DelayedRecord<K, V>> = ReferenceFactory.instance(),
    private val errorHandler: ErrorHandler = RetryingErrorHandler(),
    private val clock: Clock = Clock
) : DelayedProducer<K, V>, Closeable {

    companion object {
        private val logger = logger()
    }

    private val delayedRecordSendTasks = DelayQueue<DelayedRecordSendTask>()
    private val availableRecordSender = AvailableRecordSender()

    @Volatile
    private var closed = false

    override val numOfDelayedRecords: Int
        get() = delayedRecordSendTasks.size + availableRecordSender.recordOutboxSize

    override fun send(
        record: ProducerRecord<K, V>,
        afterDuration: Duration,
        callback: DelayedSendCallback
    ) {
        val delayedRecord = DelayedRecord(record, afterDuration, clock.now())
        logger.trace { "Delaying $record send until ${delayedRecord.scheduledOnTime}" }
        delayedRecordSendTasks += DelayedRecordSendTask(delayedRecord, callback)
    }

    override fun close() {
        if (closed) return
        logger.debug { "Closing delayed producer" }
        closed = true

        availableRecordSender.close()
        kafkaProducer.close()
    }

    private inner class DelayedRecordSendTask(
        delayedRecord: DelayedRecord<K, V>,
        val callback: DelayedSendCallback
    ) : AbstractDelayed(), SendTask<K, V> {

        private val reference: Reference<DelayedRecord<K, V>> = referenceFactory.create(delayedRecord)
        private val scheduledOnTime: Instant = delayedRecord.scheduledOnTime

        override val producerRecord: ProducerRecord<K, V> get() = reference.value.producerRecord

        override fun getDelay(): Duration = Duration.between(clock.now(), scheduledOnTime)

        override fun sent(recordMetadata: RecordMetadata) = callback.onSent(recordMetadata)

        override fun failed(exception: Exception) = callback.onFailure(exception)

        override fun discarded(cancelCause: Exception) = callback.onDiscarded(cancelCause)

        fun release() = reference.release()
    }

    private inner class AvailableRecordSender {

        private val inProgressRecordTasks = synchronizedList(mutableListOf<DelayedRecordSendTask>())
        val recordOutboxSize get() = inProgressRecordTasks.size

        private val pollingThread = thread(
            name = "kafka-delayed-producer-thread",
            isDaemon = false
        ) {
            while (!closed) {
                try {
                    if (inProgressRecordTasks.isEmpty())
                        inProgressRecordTasks += delayedRecordSendTasks.poll(1.seconds, 100)
                    if (inProgressRecordTasks.isNotEmpty())
                        sendInProgress()
                } catch (e: ClosingException) {
                } catch (e: Exception) {
                    logger.error(e) { "Polling thread caught unexpected exception" }
                }
            }
        }

        private fun sendInProgress() {
            val taskToFutureList = inProgressRecordTasks
                .map { sendTask -> sendTask to send(sendTask.producerRecord) }

            for ((sendTask, future) in taskToFutureList) {
                try {
                    val recordMetadata = future.getUntilClosed()
                    sendTask.callback.onSent(recordMetadata)
                } catch (e: ExecutionException) {
                    val cause = e.cause as Exception
                    handleException(sendTask, cause)
                }

                inProgressRecordTasks -= sendTask
                sendTask.release()
            }
        }

        private fun send(producerRecord: ProducerRecord<K, V>): Future<RecordMetadata> {
            logger.trace { "Sending record: $producerRecord" }
            return kafkaProducer.send(producerRecord)
        }

        private fun Future<RecordMetadata>.getUntilClosed(): RecordMetadata {
            while (!closed) {
                try {
                    return get(timeout = 100.millis)
                } catch (e: TimeoutException) {
                }
            }
            throw ClosingException()
        }

        private fun handleException(
            sendTask: SendTask<K, V>,
            thrownException: Exception
        ) {
            val producerRecord = sendTask.producerRecord
            logger.warn(thrownException) { "Failed to send record with exception: $thrownException" }
            logger.debug { "Failed to send record: $producerRecord" }
            try {
                errorHandler.handle(thrownException, sendTask, kafkaProducer)
                logger.debug { "Successfully handled record: $producerRecord" }
            } catch (handlingException: Exception) {
                sendTask.failed(handlingException)
            }
        }

        fun close(): Unit = pollingThread.join()
    }

    private class ClosingException : Exception()

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