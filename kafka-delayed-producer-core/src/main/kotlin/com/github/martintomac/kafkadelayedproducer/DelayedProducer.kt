package com.github.martintomac.kafkadelayedproducer

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.io.Closeable
import java.time.Duration

interface DelayedProducer<K, V> : Closeable {

    val numOfDelayedRecords: Int

    fun send(
        record: ProducerRecord<K, V>,
        afterDuration: Duration,
        callback: DelayedSendCallback
    )
}

fun <K, V> DelayedProducer<K, V>.send(
    record: ProducerRecord<K, V>,
    afterDuration: Duration
) = send(record, afterDuration, DelayedSendCallback.NO_OP)

interface DelayedSendCallback {

    companion object {
        val NO_OP = object : DelayedSendCallback {}
    }

    /**
     * Record is successfully delivered to kafka
     */
    fun onSent(recordMetadata: RecordMetadata): Unit = Unit

    /**
     * Record was failed to be delivered with `exception` as failure reason
     */
    fun onFailure(exception: Exception): Unit = Unit

    /**
     * Record was failed to be delivered and discarded by `ErrorHandler`
     */
    fun onDiscarded(cancelCause: Exception): Unit = Unit
}