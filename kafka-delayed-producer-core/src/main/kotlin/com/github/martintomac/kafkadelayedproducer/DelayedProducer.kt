package com.github.martintomac.kafkadelayedproducer

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.io.Closeable
import java.time.Duration
import java.util.concurrent.Future

interface DelayedProducer<K, V : Any> : Closeable {

    val numOfDelayedRecords: Int

    fun send(
        record: ProducerRecord<K, V>,
        afterDuration: Duration
    ) : Future<RecordMetadata>
}