package com.github.martintomac.kafkadelayedproducer

import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.io.Closeable
import java.time.Duration
import java.util.concurrent.Future

class KafkaDelayedProducer<K, V>(
    private val kafkaProducer: Producer<K, V>,
    private val referenceFactory: ReferenceFactory<DelayedRecord<K, V>> = ReferenceFactory.instance(),
    private val errorHandler: ErrorHandler = RetryingErrorHandler(),
    private val clock: Clock = Clock
) : DelayedProducer<K, V>, Closeable {

    companion object {
        private val logger = logger()
    }

    private val scheduler: Scheduler<K, V> = DefaultScheduler(
        referenceFactory,
        clock
    ) { sendTask -> executeSendTask(sendTask) }

    @Volatile
    private var closed = false

    override val numOfDelayedRecords: Int
        get() = scheduler.numOfDelayedRecords

    override fun send(
        record: ProducerRecord<K, V>,
        afterDuration: Duration,
        callback: DelayedSendCallback
    ) {
        val delayedRecord = DelayedRecord(record, afterDuration, clock.now())
        logger.trace { "Delaying $record send until ${delayedRecord.scheduledOnTime}" }
        scheduler.schedule(delayedRecord, callback)
    }

    private fun executeSendTask(
        sendTask: SendTask<K, V>
    ): Future<RecordMetadata> {
        val producerRecord = sendTask.producerRecord
        logger.trace { "Sending record: $producerRecord" }

        return kafkaProducer.send(producerRecord) { recordMetadata, exception ->
            if (exception != null) {
                handleException(sendTask, exception)
            } else {
                sendTask.sent(recordMetadata)
            }
        }
    }

    private fun handleException(
        sendTask: SendTask<K, V>,
        thrownException: Exception
    ) {
        val producerRecord = sendTask.producerRecord
        logger.warn(thrownException) { "Failed to send record with exception: $thrownException" }
        logger.debug { "Failed to send record: $producerRecord" }
        try {
            errorHandler.handle(thrownException, sendTask, kafkaProducer)
            logger.debug { "Successfully handled record: $producerRecord" }
        } catch (handlingException: Exception) {
            sendTask.failed(handlingException)
        }
    }

    override fun close() {
        if (closed) return
        closed = true
        scheduler.close()
        kafkaProducer.close()
    }
}