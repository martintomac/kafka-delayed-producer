package com.github.martintomac.kafkadelayedproducer

import java.io.Closeable
import java.time.Instant
import java.util.concurrent.Future

interface Scheduler<T> : Closeable {

    val numberOfScheduledTasks: Int

    fun schedule(
        task: Task<T>,
        time: Instant
    )

    override fun close() {}

    class Task<T>(
        val data: T,
        val job: Job<T>
    )

    fun interface Job<T> : (JobContext<T>) -> Future<*>

    interface JobContext<T> {
        val data: T
        val cancelled: Boolean
    }
}