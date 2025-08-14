package com.github.martintomac.kafkadelayedproducer

import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Instant

data class DelayedRecord<K, V>(
    val producerRecord: ProducerRecord<K, V>,
    val time: Instant
)