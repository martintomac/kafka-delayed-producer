package com.github.martintomac.kafkadelayedproducer

import java.io.Closeable

interface Scheduler<K, V> : Closeable {

    val numOfDelayedRecords: Int

    fun schedule(
        delayedRecord: DelayedRecord<K, V>,
        callback: DelayedSendCallback
    )

    override fun close() {}
}