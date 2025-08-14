package com.github.martintomac.kafkadelayedproducer.directories

import com.github.martintomac.kafkadelayedproducer.*
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.assertTrue

internal class MapDirectoryDelayedProducerTest {

    private val mockProducer = MockProducer(true, StringSerializer(), StringSerializer())
    private val mapDirectory = MapDirectory.create<DelayedRecord<String, String>> { ConcurrentHashMap() }
    private val delayedProducer = KafkaDelayedProducer(
        kafkaProducer = mockProducer,
        scheduler = DefaultScheduler(DirectoryReferenceFactory(mapDirectory))
    )

    @AfterEach
    internal fun tearDown() {
        delayedProducer.close()
    }

    @Test
    fun sent_after_delay() {
        val record = ProducerRecord("test-topic", "test-key", "test-value")
        delayedProducer.send(record after 100.millis)

        await atMost 200.millis until { mockProducer.history().size > 0 }
    }

    @Test
    fun not_sent_before_delay_expired() {
        val record = ProducerRecord("test-topic", "test-key", "test-value")
        delayedProducer.send(record after 200.millis)

        sleep(100.millis)
        assertTrue { mockProducer.history().size == 0 }
    }

    @Test
    fun sent_many_after_delay() {
        for (i in 1..100) {
            val record = ProducerRecord("test-topic", "test-key-$i", "test-value-$i")
            delayedProducer.send(record after 100.millis)
        }

        await atMost 500.millis until { mockProducer.history().size == 100 }
    }

    private fun sleep(duration: Duration) {
        Thread.sleep(duration.toMillis())
    }
}