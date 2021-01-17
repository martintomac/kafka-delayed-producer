package com.github.martintomac.kafkadelayedproducer

import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.serialization.StringSerializer
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.awaitility.kotlin.withPollDelay
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit.NANOSECONDS
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.TimeoutException
import kotlin.test.assertFalse
import kotlin.test.assertTrue

internal class KafkaDelayedProducerTest {

    lateinit var delayedProducer: DelayedProducer<String, String>

    @AfterEach
    internal fun tearDown() {
        delayedProducer.close()
    }

    @Test
    fun sent_after_delay() {
        val mockProducer = MockProducer(true, StringSerializer(), StringSerializer())
        delayedProducer = KafkaDelayedProducer(mockProducer)

        val record = ProducerRecord("test-topic", "test-key", "test-value")
        val future = delayedProducer.sendAsync(record after 100.millis)

        await atMost 200.millis until { future.isDone }
    }

    @Test
    fun not_sent_before_delay_expired() {
        val mockProducer = MockProducer(true, StringSerializer(), StringSerializer())
        delayedProducer = KafkaDelayedProducer(mockProducer)

        val record = ProducerRecord("test-topic", "test-key", "test-value")
        val future = delayedProducer.sendAsync(record after 200.millis)

        sleep(100.millis)
        assertFalse { future.isDone }
    }

    @Test
    fun sent_many_after_delay() {
        val mockProducer = MockProducer(true, StringSerializer(), StringSerializer())
        delayedProducer = KafkaDelayedProducer(mockProducer)

        for (i in 1..100) {
            val record = ProducerRecord("test-topic", "test-key-$i", "test-value-$i")
            delayedProducer.send(record after 100.millis)
        }

        await atMost 500.millis until { mockProducer.history().size == 100 }
    }

    @Test
    fun no_scheduled_records() {
        val mockProducer = MockProducer(true, StringSerializer(), StringSerializer())
        delayedProducer = KafkaDelayedProducer(mockProducer)

        assertTrue { delayedProducer.numOfDelayedRecords == 0 }
    }

    @Test
    fun has_scheduled_records() {
        val mockProducer = MockProducer(true, StringSerializer(), StringSerializer())
        delayedProducer = KafkaDelayedProducer(mockProducer)

        val record = ProducerRecord("test-topic", "test-key", "test-value")
        delayedProducer.send(record after 100.millis)

        assertTrue { delayedProducer.numOfDelayedRecords == 1 }
    }

    @Test
    fun sent_scheduled_record() {
        val mockProducer = MockProducer(true, StringSerializer(), StringSerializer())
        delayedProducer = KafkaDelayedProducer(mockProducer)

        val record = ProducerRecord("test-topic", "test-key", "test-value")
        delayedProducer.send(record after 100.millis)

        sleep(200.millis)

        assertTrue { delayedProducer.numOfDelayedRecords == 0 }
    }

    @Test
    fun recover_on_failure() {
        val mockProducer = MockProducer(false, StringSerializer(), StringSerializer())
        delayedProducer = KafkaDelayedProducer(mockProducer)

        val record = ProducerRecord("test-topic", "test-key", "test-value")
        val future = delayedProducer.sendAsync(record after 100.millis)

        await atMost 200.millis withPollDelay 10.millis until { mockProducer.history().size == 1 }
        assertFalse { future.isDone }
        assertTrue { delayedProducer.numOfDelayedRecords == 1 }

        mockProducer.errorNext(KafkaException("Some kafka exception"))

        await atMost 5.seconds until { mockProducer.history().size == 2 }
        assertFalse { future.isDone }
        assertTrue { delayedProducer.numOfDelayedRecords == 1 }

        mockProducer.completeNext()

        await atMost 2.seconds until { future.isDone }
        assertTrue { delayedProducer.numOfDelayedRecords == 0 }
    }

    @Test
    @Timeout(value = 1, unit = SECONDS)
    fun blocking_future_get_should_return_result() {
        val mockProducer = MockProducer(true, StringSerializer(), StringSerializer())
        delayedProducer = KafkaDelayedProducer(mockProducer)

        val record = ProducerRecord("test-topic", "test-key", "test-value")
        val future = delayedProducer.sendAsync(record after 100.millis)

        val recordMetadata = future.get()
        assertTrue { recordMetadata != null }
    }

    @Test
    fun future_get_should_timeout() {
        val mockProducer = MockProducer(true, StringSerializer(), StringSerializer())
        delayedProducer = KafkaDelayedProducer(mockProducer)

        val record = ProducerRecord("test-topic", "test-key", "test-value")
        val future = delayedProducer.sendAsync(record after 100.millis)

        assertThrows<TimeoutException> { future.get(50.millis) }
    }

    @Test
    fun future_get_with_timeout_should_return() {
        val mockProducer = MockProducer(true, StringSerializer(), StringSerializer())
        delayedProducer = KafkaDelayedProducer(mockProducer)

        val record = ProducerRecord("test-topic", "test-key", "test-value")
        val future = delayedProducer.sendAsync(record after 100.millis)

        future.get(200.millis)
    }

    private fun sleep(duration: Duration): Unit = Thread.sleep(duration.toMillis())

    private fun <T> Future<T>.get(timeout: Duration): T = get(timeout.toNanos(), NANOSECONDS)
}