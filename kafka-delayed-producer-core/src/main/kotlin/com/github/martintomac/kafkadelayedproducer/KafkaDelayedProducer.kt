package com.github.martintomac.kafkadelayedproducer

import com.github.martintomac.kafkadelayedproducer.Scheduler.Task
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.io.Closeable
import java.lang.ref.WeakReference
import java.time.Duration
import java.util.concurrent.*

class KafkaDelayedProducer<K, V>(
    private val kafkaProducer: Producer<K, V>,
    private val scheduler: Scheduler<ProducerRecord<K, V>> = DefaultScheduler(),
    private val errorHandler: ErrorHandler = RetryingErrorHandler(),
    private val clock: Clock = Clock
) : DelayedProducer<K, V>, Closeable {

    companion object {
        private val logger = logger()
    }

    @Volatile
    private var closed = false

    override val numOfDelayedRecords: Int
        get() = scheduler.numberOfScheduledTasks

    override fun send(
        record: ProducerRecord<K, V>,
        afterDuration: Duration,
        callback: DelayedSendCallback
    ) {
        val schedulingToTime = clock.now() + afterDuration
        logger.trace { "Delaying $record send until $schedulingToTime" }

        val job = createSendTaskJob(callback)

        scheduler.schedule(
            Task(record, job),
            schedulingToTime
        )
    }

    private fun createSendTaskJob(
        callback: DelayedSendCallback
    ): Scheduler.Job<ProducerRecord<K, V>> {
        val callbackReference = WeakReference(callback)
        return Scheduler.Job { sendRecord(it, callbackReference.get() ?: DelayedSendCallback.NO_OP) }
    }

    private fun sendRecord(
        jobContext: Scheduler.JobContext<ProducerRecord<K, V>>,
        callback: DelayedSendCallback
    ): Future<*> {
        val record = jobContext.data

        logger.trace { "Sending record: $record" }

        val sendTask = SendTaskImpl(record, callback)
        val future = kafkaProducer.send(record) // TODO use kafkaProducer internal concurrency
        while (!jobContext.cancelled) {
            try {
                val recordMetadata = future.get(timeout = 100.millis)
                sendTask.sent(recordMetadata)
                return CompletableFuture.completedFuture(Unit)
            } catch (_: TimeoutException) {
            } catch (e: Exception) {
                handleException(sendTask, e)
                return CompletableFuture.completedFuture(Unit)
            }
        }
        throw CancellationException()
    }

    private fun <T> Future<T>.get(timeout: Duration): T = get(timeout.toNanos(), TimeUnit.NANOSECONDS)

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

    override fun close() {
        if (closed) return
        closed = true
        scheduler.close()
        kafkaProducer.close()
    }

    private class SendTaskImpl<K, V>(
        override val producerRecord: ProducerRecord<K, V>,
        private val callback: DelayedSendCallback
    ) : SendTask<K, V> {

        override fun sent(recordMetadata: RecordMetadata) = callback.onSent(recordMetadata)

        override fun failed(exception: Exception) = callback.onFailure(exception)

        override fun discarded(cancelCause: Exception) = callback.onDiscarded(cancelCause)
    }
}