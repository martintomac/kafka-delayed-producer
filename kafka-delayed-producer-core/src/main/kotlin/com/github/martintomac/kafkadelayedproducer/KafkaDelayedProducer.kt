package com.github.martintomac.kafkadelayedproducer

import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.io.Closeable
import java.lang.ref.WeakReference
import java.time.Duration
import java.time.Instant
import java.util.Collections.emptyList
import java.util.Collections.synchronizedList
import java.util.concurrent.*
import java.util.concurrent.TimeUnit.NANOSECONDS
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock

class KafkaDelayedProducer<K, V : Any>(
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
        afterDuration: Duration
    ): Future<RecordMetadata> {
        val delayedRecord = DelayedRecord(record, afterDuration, clock.now())
        logger.trace { "Delaying $record send until ${delayedRecord.scheduledOnTime}" }
        return schedule(delayedRecord)
    }

    private fun schedule(delayedRecord: DelayedRecord<K, V>): Future<RecordMetadata> {
        val future: SendTaskFuture<RecordMetadata> = SendTaskFuture()
        delayedRecordSendTasks += DelayedRecordSendTask(delayedRecord, future)
        return future
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
        future: SendTaskFuture<RecordMetadata>
    ) : AbstractDelayed() {

        private val reference: Reference<DelayedRecord<K, V>> = referenceFactory.create(delayedRecord)
        private val scheduledOnTime: Instant = delayedRecord.scheduledOnTime

        private val futureReference = WeakReference(future)

        val producerRecord: ProducerRecord<K, V> get() = reference.value.producerRecord

        override fun getDelay(): Duration = Duration.between(clock.now(), scheduledOnTime)

        fun completed(recordMetadata: RecordMetadata) = futureReference.get()?.complete(recordMetadata)

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
                    val recordTasks = delayedRecordSendTasks.poll(1.seconds, 100)
                    if (recordTasks.isNotEmpty()) send(recordTasks)
                } catch (e: ClosingException) {
                } catch (e: Exception) {
                    logger.error(e) { "Polling thread caught unexpected exception" }
                }
            }
        }

        private fun send(recordSendTasks: List<DelayedRecordSendTask>) {
            inProgressRecordTasks += recordSendTasks

            val taskToFutureList = recordSendTasks
                .map { sendTask -> sendTask to send(sendTask.producerRecord) }

            for ((sendTask, future) in taskToFutureList) {
                val recordMetadata: RecordMetadata =
                    try {
                        future.getUntilClosed()
                    } catch (e: ExecutionException) {
                        val cause = e.cause as Exception
                        handleException(sendTask.producerRecord, cause)
                    }

                sendTask.completed(recordMetadata)
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
            producerRecord: ProducerRecord<K, V>,
            thrownException: Exception
        ): RecordMetadata {
            logger.warn(thrownException) { "Failed to send record with exception: $thrownException" }
            logger.debug { "Failed to send record: $producerRecord" }
            while (!closed) {
                try {
                    return errorHandler.handle(thrownException, producerRecord, kafkaProducer)
                        .also { logger.debug { "Handled failed send of record: $producerRecord" } }
                } catch (handlingException: Exception) {
                    logger.warn(handlingException) { "Failed to handle record send with exception: $handlingException" }
                }
            }
            throw ClosingException()
        }

        fun close(): Unit = pollingThread.join()
    }

    private class SendTaskFuture<T : Any> : Future<T> {

        private val lock = ReentrantLock()
        private val condition = lock.newCondition()

        @Volatile
        private var result: T? = null

        fun complete(result: T): Unit = lock.withLock {
            this.result = result
            condition.signalAll()
        }

        override fun cancel(mayInterruptIfRunning: Boolean): Boolean = false

        override fun isCancelled(): Boolean = false

        override fun isDone(): Boolean = result != null

        override fun get(): T = lock.withLock {
            if (result == null) condition.await()
            return result!!
        }

        override fun get(timeout: Long, unit: TimeUnit): T = lock.withLock {
            if (result == null) {
                val signalled = condition.await(timeout, unit)
                if (!signalled) throw TimeoutException()
            }
            return result!!
        }
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