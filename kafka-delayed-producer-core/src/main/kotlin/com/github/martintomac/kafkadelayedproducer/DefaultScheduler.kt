package com.github.martintomac.kafkadelayedproducer

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.*
import kotlin.concurrent.thread

class DefaultScheduler<K, V>(
    private val referenceFactory: ReferenceFactory<DelayedRecord<K, V>> = ReferenceFactory.instance(),
    private val clock: Clock = Clock,
    private val producer: (SendTask<K, V>) -> Future<RecordMetadata>
) : Scheduler<K, V> {

    companion object {
        private val logger = logger()
    }

    private val delayedRecordSendTasks = DelayQueue<DelayedRecordSendTask>()
    private val availableRecordSender = AvailableRecordSender()

    @Volatile
    private var closed = false

    override val numOfDelayedRecords: Int
        get() = delayedRecordSendTasks.size + availableRecordSender.recordOutboxSize

    override fun schedule(
        delayedRecord: DelayedRecord<K, V>,
        callback: DelayedSendCallback
    ) {
        delayedRecordSendTasks += DelayedRecordSendTask(delayedRecord, callback)
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

        private val inProgressRecordTasks = Collections.synchronizedList(mutableListOf<DelayedRecordSendTask>())
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
                } catch (_: ClosingException) {
                } catch (e: Exception) {
                    logger.error(e) { "Polling thread caught unexpected exception" }
                }
            }
        }

        private fun <T : Delayed> DelayQueue<T>.poll(
            timeout: Duration,
            limit: Int
        ): List<T> {
            val elements = mutableListOf<T>()

            drainTo(elements, limit)
            if (elements.isNotEmpty()) return elements

            val recordReference = poll(timeout.toNanos(), TimeUnit.NANOSECONDS)
                ?: return Collections.emptyList()

            elements += recordReference
            if (limit > 1) drainTo(elements, limit - 1)
            return elements
        }

        private fun sendInProgress() {
            val taskToFutureList = inProgressRecordTasks
                .map { sendTask -> sendTask to producer(sendTask) }

            for ((sendTask, future) in taskToFutureList) {
                future.joinUntilClosed()

                inProgressRecordTasks -= sendTask
                sendTask.release()
            }
        }

        private fun Future<RecordMetadata>.joinUntilClosed() {
            while (!closed) {
                try {
                    get(timeout = 100.millis)
                } catch (_: ExecutionException) {
                    return
                } catch (_: TimeoutException) {
                }
            }
            throw ClosingException()
        }

        fun close(): Unit = pollingThread.join()
    }

    override fun close() {
        if (closed) return
        logger.debug { "Closing delayed producer" }
        closed = true
        availableRecordSender.close()
    }

    private fun <T> Future<T>.get(timeout: Duration): T = get(timeout.toNanos(), TimeUnit.NANOSECONDS)
}

private class ClosingException : Exception()

private abstract class AbstractDelayed : Delayed {

    override fun compareTo(other: Delayed): Int {
        return getDelay(TimeUnit.NANOSECONDS)
            .compareTo(other.getDelay(TimeUnit.NANOSECONDS))
    }

    override fun getDelay(unit: TimeUnit): Long {
        return unit.convert(getDelay().toNanos(), TimeUnit.NANOSECONDS)
    }

    abstract fun getDelay(): Duration
}