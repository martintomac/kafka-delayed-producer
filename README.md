Kafka Delayed Producer
=================
Kafka delayed producer enables you to produce records with delay. 
It keeps a list of records in-memory and produces them using event loop with given delay.

### Example ###

```kotlin
fun main() {
    // init producer with apache kafka producer
    val delayedProducer = KafkaDelayedProducer(kafkaProducer)

    val record = ProducerRecord(topic, key, value)
    // produce record with delay    
    val future = delayedProducer.send(record after 100.millis)
}
```

For additional examples you can check tests

## External record storing ##

By default, records are referenced directly i.e. stored within producer.
You can configure a producer to indirectly reference records and by that keep them outside a producer.
With that, records can be kept off-heap, on disk or in an external database.

To use an external record storing you have to provide `Directory` implementation which stores records.

Out of the box you get `MapDirectory` implementation of `Directory` which keeps records in provided `Map`. 
This implementation is useful since there are many third party Map implementations which keep data on disk.

### Example ###

```kotlin
fun main() {
    // init directory which will store records given delays
    val directory: Directory = MapDirectory.create<DelayedRecord<String, String>> { ConcurrentHashMap() }
    // use indirect reference factory which uses given directory
    val delayedProducer = KafkaDelayedProducer(
        kafkaProducer = kafkaProducer,
        referenceFactory = DirectoryReferenceFactory(directory)
    )
}
```