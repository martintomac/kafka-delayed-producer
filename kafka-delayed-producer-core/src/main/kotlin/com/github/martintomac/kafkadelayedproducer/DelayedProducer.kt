package com.github.martintomac.kafkadelayedproducer

import org.apache.kafka.clients.producer.ProducerRecord
import java.io.Closeable
import java.time.Duration

interface DelayedProducer<K, V : Any> : Closeable {

    val numOfUnsentRecords: Int

    fun send(
        record: ProducerRecord<K, V>,
        afterDuration: Duration
    )
}