package com.github.martintomac.kafkadelayedproducer

import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.time.Duration

interface ErrorHandler {

    fun <K, V> handle(
        ex: Exception,
        sendTask: SendTask<K, V>,
        producer: Producer<K, V>
    )
}

interface SendTask<K, V> {

    val producerRecord: ProducerRecord<K, V>

    fun sent(recordMetadata: RecordMetadata)

    fun failed(exception: Exception)

    fun discarded(cancelCause: Exception)
}

class RetryingErrorHandler(
    private val backOff: BackOff = FixedBackOff.DEFAULT
) : ErrorHandler {

    companion object {
        private val logger = logger()
    }

    override fun <K, V> handle(
        ex: Exception,
        sendTask: SendTask<K, V>,
        producer: Producer<K, V>
    ) {
        val producerRecord = sendTask.producerRecord
        with(backOff.start()) {
            while (true) {
                val nextBackOff = nextBackOff()
                if (nextBackOff == null) {
                    logger.warn { "Backing off recover of record ($producerRecord) after max attempts exceeded" }
                    sendTask.discarded(BackedOffException("Failed to recover record ($producerRecord)"))
                    return
                }
                sleep(nextBackOff)

                try {
                    val recordMetadata = producer.send(producerRecord).get()
                    sendTask.sent(recordMetadata)
                    return
                } catch (e: Exception) {
                    logger.warn(e) { "Failed to recover record ($producerRecord)" }
                }
            }
        }
    }

    private fun sleep(duration: Duration) {
        Thread.sleep(duration.toMillis())
    }

    interface BackOff {
        fun start(): BackOffExecution
    }

    interface BackOffExecution {
        fun nextBackOff(): Duration?
    }

    class FixedBackOff(
        private val interval: Duration,
        private val maxAttempts: Long
    ) : BackOff {

        companion object {
            val DEFAULT = FixedBackOff(1.seconds, Long.MAX_VALUE)
        }

        override fun start() = object : BackOffExecution {
            private var currentAttempt = 0L

            override fun nextBackOff() = interval.takeIf { currentAttempt++ < maxAttempts }
        }
    }

    class BackedOffException(message: String) : Exception(message)
}