package com.github.martintomac.kafkadelayedproducer

import org.slf4j.Logger
import org.slf4j.LoggerFactory

@Suppress("unused") // receiver is needed for reified type
inline fun <reified T> T.logger(): Logger = LoggerFactory.getLogger(T::class.java)

private class LazyString(private val msg: () -> String) {
    override fun toString(): String = msg()
}

fun Logger.trace(msg: () -> String) {
    trace("{}", LazyString(msg))
}

fun Logger.trace(t: Throwable, msg: () -> String) {
    trace("{}", LazyString(msg), t)
}

fun Logger.debug(msg: () -> String) {
    debug("{}", LazyString(msg))
}

fun Logger.debug(t: Throwable, msg: () -> String) {
    debug("{}", LazyString(msg), t)
}

fun Logger.info(msg: () -> String) {
    info("{}", LazyString(msg))
}

fun Logger.info(t: Throwable, msg: () -> String) {
    info("{}", LazyString(msg), t)
}

fun Logger.warn(msg: () -> String) {
    warn("{}", LazyString(msg))
}

fun Logger.warn(t: Throwable, msg: () -> String) {
    warn("{}", LazyString(msg), t)
}

fun Logger.error(msg: () -> String) {
    error("{}", LazyString(msg))
}

fun Logger.error(t: Throwable, msg: () -> String) {
    error("{}", LazyString(msg), t)
}