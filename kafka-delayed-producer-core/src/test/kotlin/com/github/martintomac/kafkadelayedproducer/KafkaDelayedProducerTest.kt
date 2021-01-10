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
import java.time.Duration
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
        delayedProducer.send(record after 100.millis)

        await atMost 200.millis until { mockProducer.history().size > 0 }
    }

    @Test
    fun not_sent_before_delay_expired() {
        val mockProducer = MockProducer(true, StringSerializer(), StringSerializer())
        delayedProducer = KafkaDelayedProducer(mockProducer)

        val record = ProducerRecord("test-topic", "test-key", "test-value")
        delayedProducer.send(record after 200.millis)

        sleep(100.millis)
        assertTrue { mockProducer.history().size == 0 }
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

        assertTrue { delayedProducer.numOfUnsentRecords == 0 }
    }

    @Test
    fun has_scheduled_records() {
        val mockProducer = MockProducer(true, StringSerializer(), StringSerializer())
        delayedProducer = KafkaDelayedProducer(mockProducer)

        val record = ProducerRecord("test-topic", "test-key", "test-value")
        delayedProducer.send(record after 100.millis)

        assertTrue { delayedProducer.numOfUnsentRecords == 1 }
    }

    @Test
    fun sent_scheduled_record() {
        val mockProducer = MockProducer(true, StringSerializer(), StringSerializer())
        delayedProducer = KafkaDelayedProducer(mockProducer)

        val record = ProducerRecord("test-topic", "test-key", "test-value")
        delayedProducer.send(record after 100.millis)

        sleep(200.millis)

        assertTrue { delayedProducer.numOfUnsentRecords == 0 }
    }

    @Test
    fun recover_on_failure() {
        val mockProducer = MockProducer(false, StringSerializer(), StringSerializer())
        delayedProducer = KafkaDelayedProducer(mockProducer)

        val record = ProducerRecord("test-topic", "test-key", "test-value")
        delayedProducer.send(record after 10.millis)

        await atMost 100.millis withPollDelay 10.millis until { mockProducer.history().size == 1 }
        assertTrue { delayedProducer.numOfUnsentRecords == 1 }

        mockProducer.errorNext(KafkaException("Some kafka exception"))

        await atMost 2.seconds until { mockProducer.history().size == 2 }
        assertTrue { delayedProducer.numOfUnsentRecords == 1 }

        mockProducer.completeNext()

        await atMost 2.seconds until { delayedProducer.numOfUnsentRecords == 0 }
    }

    private fun sleep(duration: Duration) {
        Thread.sleep(duration.toMillis())
    }
}