package com.github.martintomac.kafkadelayedproducer

import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertTrue

internal class DurationsTest {

    @Test
    internal fun round_millis_conversions() {
        assertTrue { 100.millis == Duration.ofMillis(100) }
        assertTrue { 100L.millis == Duration.ofMillis(100) }
    }

    @Test
    internal fun round_seconds_conversions() {
        assertTrue { 10.seconds == Duration.ofSeconds(10) }
        assertTrue { 10L.seconds == Duration.ofSeconds(10) }
        assertTrue { 10.0.seconds == Duration.ofSeconds(10) }
    }

    @Test
    internal fun double_seconds_conversions() {
        assertTrue { 0.5.seconds == Duration.ofMillis(500) }
    }

    @Test
    internal fun round_minutes_conversions() {
        assertTrue { 10.minutes == Duration.ofMinutes(10) }
        assertTrue { 10L.minutes == Duration.ofMinutes(10) }
        assertTrue { 10.0.minutes == Duration.ofMinutes(10) }
    }

    @Test
    internal fun double_minutes_conversions() {
        assertTrue { 0.5.minutes == Duration.ofSeconds(30) }
    }

    @Test
    internal fun round_hours_conversions() {
        assertTrue { 10.hours == Duration.ofHours(10) }
        assertTrue { 10L.hours == Duration.ofHours(10) }
        assertTrue { 10.0.hours == Duration.ofHours(10) }
    }

    @Test
    internal fun double_hours_conversions() {
        assertTrue { 0.5.hours == Duration.ofMinutes(30) }
    }

    @Test
    internal fun round_days_conversions() {
        assertTrue { 10.days == Duration.ofDays(10) }
        assertTrue { 10L.days == Duration.ofDays(10) }
        assertTrue { 10.0.days == Duration.ofDays(10) }
    }

    @Test
    internal fun double_days_conversions() {
        assertTrue { 0.5.days == Duration.ofHours(12) }
    }
}