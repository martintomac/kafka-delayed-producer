package com.github.martintomac.kafkadelayedproducer

import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration
import java.time.Instant

data class DelayedRecord<K, V: Any>(
    val producerRecord: ProducerRecord<K, V>,
    val delay: Duration,
    val timeOfScheduling: Instant
) {
    val scheduledOnTime: Instant get() = timeOfScheduling + delay
}