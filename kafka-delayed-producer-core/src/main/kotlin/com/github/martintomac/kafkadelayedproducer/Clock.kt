package com.github.martintomac.kafkadelayedproducer

import java.time.Instant

interface Clock {

    companion object : Clock {
        override fun now(): Instant = Instant.now()
    }

    fun now(): Instant
}