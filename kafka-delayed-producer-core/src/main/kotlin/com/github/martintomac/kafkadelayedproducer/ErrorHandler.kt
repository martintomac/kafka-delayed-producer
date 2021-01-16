package com.github.martintomac.kafkadelayedproducer

import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.time.Duration

interface ErrorHandler {

    fun <K, V> handle(
        ex: Exception,
        producerRecord: ProducerRecord<K, V>,
        producer: Producer<K, V>
    ): RecordMetadata
}

class RetryingErrorHandler(
    private val retryDuration: Duration = 1.seconds
) : ErrorHandler {

    override fun <K, V> handle(
        ex: Exception,
        producerRecord: ProducerRecord<K, V>,
        producer: Producer<K, V>
    ): RecordMetadata {
        sleep(retryDuration)
        return producer.send(producerRecord).get()
    }

    private fun sleep(duration: Duration) {
        Thread.sleep(duration.toMillis())
    }
}