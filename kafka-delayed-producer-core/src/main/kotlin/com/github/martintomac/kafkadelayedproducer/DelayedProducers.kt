package com.github.martintomac.kafkadelayedproducer

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.time.Duration
import java.util.concurrent.CompletableFuture

fun <K, V> DelayedProducer<K, V>.send(
    delayedProductRecord: DelayedProductRecord<K, V>
) = send(
    record = delayedProductRecord.record,
    afterDuration = delayedProductRecord.delay
)

fun <K, V> DelayedProducer<K, V>.send(
    delayedProductRecordWithCallback: DelayedProductRecordWithCallback<K, V>
): Unit = send(
    record = delayedProductRecordWithCallback.record,
    afterDuration = delayedProductRecordWithCallback.delay,
    callback = delayedProductRecordWithCallback.callback
)

fun <K, V> DelayedProducer<K, V>.sendAsync(
    record: ProducerRecord<K, V>,
    afterDuration: Duration,
): CompletableFuture<RecordMetadata> {
    val future = CompletableFuture<RecordMetadata>()
    send(
        record = record,
        afterDuration = afterDuration,
        callback = CompletableFutureCallback(future)
    )
    return future
}

private class CompletableFutureCallback(
    private val future: CompletableFuture<RecordMetadata>
) : DelayedSendCallback {

    override fun onSent(recordMetadata: RecordMetadata) {
        future.complete(recordMetadata)
    }

    override fun onFailure(exception: Exception) {
        future.completeExceptionally(exception)
    }

    override fun onDiscarded(cancelCause: Exception) {
        future.completeExceptionally(cancelCause)
    }
}

fun <K, V> DelayedProducer<K, V>.sendAsync(
    delayedProductRecord: DelayedProductRecord<K, V>
): CompletableFuture<RecordMetadata> = sendAsync(
    record = delayedProductRecord.record,
    afterDuration = delayedProductRecord.delay
)

infix fun <K, V> ProducerRecord<K, V>.after(duration: Duration) = DelayedProductRecord(this, duration)

data class DelayedProductRecord<K, V>(
    val record: ProducerRecord<K, V>,
    val delay: Duration
)

infix fun <K, V> DelayedProductRecord<K, V>.withCallback(callback: DelayedSendCallback) =
    DelayedProductRecordWithCallback(record, delay, callback)

data class DelayedProductRecordWithCallback<K, V>(
    val record: ProducerRecord<K, V>,
    val delay: Duration,
    val callback: DelayedSendCallback
)