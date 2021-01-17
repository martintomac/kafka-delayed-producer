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
    private val retryDuration: Duration = 1.seconds
) : ErrorHandler {

    override fun <K, V> handle(
        ex: Exception,
        sendTask: SendTask<K, V>,
        producer: Producer<K, V>
    ) {
        sleep(retryDuration)
        val recordMetadata = producer.send(sendTask.producerRecord).get()
        sendTask.sent(recordMetadata)
    }

    private fun sleep(duration: Duration) {
        Thread.sleep(duration.toMillis())
    }
}