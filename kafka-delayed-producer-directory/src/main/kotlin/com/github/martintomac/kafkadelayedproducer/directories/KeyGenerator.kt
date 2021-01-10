package com.github.martintomac.kafkadelayedproducer.directories

import java.util.concurrent.atomic.AtomicLong

fun interface KeyGenerator<K> {
    fun next(): K
}

class LongKeyGenerator(start: Long = 0) : KeyGenerator<Long> {

    private val nextId = AtomicLong(start)

    override fun next() = nextId.getAndIncrement() // no need to check for overflow since Long is large enough
}