package com.github.martintomac.kafkadelayedproducer

import java.time.Duration

inline val Int.millis: Duration get() = toLong().millis
inline val Long.millis: Duration get() = Duration.ofMillis(this)

inline val Int.seconds: Duration get() = toLong().seconds
inline val Long.seconds: Duration get() = Duration.ofSeconds(this)
inline val Double.seconds: Duration get() = (this * 1000).toLong().millis

inline val Int.minutes: Duration get() = toLong().minutes
inline val Long.minutes: Duration get() = Duration.ofMinutes(this)
inline val Double.minutes: Duration get() = (this * 60).seconds

inline val Int.hours: Duration get() = toLong().hours
inline val Long.hours: Duration get() = Duration.ofHours(this)
inline val Double.hours: Duration get() = (this * 60).minutes

inline val Int.days: Duration get() = toLong().days
inline val Long.days: Duration get() = Duration.ofDays(this)
inline val Double.days: Duration get() = (this * 24).hours