package com.github.martintomac.kafkadelayedproducer

import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration

fun <K, V : Any> DelayedProducer<K, V>.send(delayedProductRecord: DelayedProductRecord<K, V>) =
    send(delayedProductRecord.record, delayedProductRecord.delay)

infix fun <K, V : Any> ProducerRecord<K, V>.after(duration: Duration): DelayedProductRecord<K, V> =
    DelayedProductRecord(this, duration)

data class DelayedProductRecord<K, V : Any>(
    val record: ProducerRecord<K, V>,
    val delay: Duration
)