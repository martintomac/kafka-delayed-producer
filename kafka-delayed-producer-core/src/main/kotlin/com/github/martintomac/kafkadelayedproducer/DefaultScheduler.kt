package com.github.martintomac.kafkadelayedproducer

import com.github.martintomac.kafkadelayedproducer.Scheduler.Task
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration
import java.time.Instant
import java.util.Collections.emptyList
import java.util.Collections.synchronizedList
import java.util.concurrent.CancellationException
import java.util.concurrent.DelayQueue
import java.util.concurrent.Delayed
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.NANOSECONDS
import kotlin.concurrent.thread

class DefaultScheduler<K, V>(
    private val referenceFactory: ReferenceFactory<DelayedRecord<K, V>> = ReferenceFactory.instance(),
    private val clock: Clock = Clock
) : Scheduler<ProducerRecord<K, V>> {

    companion object {
        private val logger = logger()
    }

    private val delayedRecordSendTasks = DelayQueue<DelayedRecordSendTask>()
    private val inProgressRecordTasks = synchronizedList(mutableListOf<DelayedRecordSendTask>())

    @Volatile
    private var closed = false

    override val numberOfScheduledTasks: Int
        get() = delayedRecordSendTasks.size + inProgressRecordTasks.size

    override fun schedule(
        task: Task<ProducerRecord<K, V>>,
        time: Instant,
    ) {
        delayedRecordSendTasks += DelayedRecordSendTask(
            record = task.data,
            scheduledOnTime = time,
            job = task.job
        )
    }

    private inner class DelayedRecordSendTask(
        record: ProducerRecord<K, V>,
        private val scheduledOnTime: Instant,
        private val job: Scheduler.Job<ProducerRecord<K, V>>
    ) : AbstractDelayed() {

        private val reference: Reference<DelayedRecord<K, V>> =
            referenceFactory.create(DelayedRecord(record, scheduledOnTime))

        override fun getDelay(): Duration = Duration.between(clock.now(), scheduledOnTime)

        fun startExecution() = job(JobContextImpl(reference.value.producerRecord))

        fun release() = reference.release()
    }

    private inner class JobContextImpl(
        override val data: ProducerRecord<K, V>
    ) : Scheduler.JobContext<ProducerRecord<K, V>> {
        override val cancelled: Boolean
            get() = closed
    }

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
            } catch (_: CancellationException) {
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

        val recordReference = poll(timeout.toNanos(), NANOSECONDS)
            ?: return emptyList()

        elements += recordReference
        if (limit > 1) drainTo(elements, limit - 1)
        return elements
    }

    private fun sendInProgress() {
        val taskToFutureList = inProgressRecordTasks
            .map { sendTask -> sendTask to sendTask.startExecution() }

        for ((sendTask, future) in taskToFutureList) {
            try {
                future.get()
                inProgressRecordTasks -= sendTask
                sendTask.release()
            } catch (_: CancellationException) {
            }
        }
    }

    override fun close() {
        if (closed) return
        logger.debug { "Closing delayed producer" }
        closed = true
        pollingThread.join()
    }

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