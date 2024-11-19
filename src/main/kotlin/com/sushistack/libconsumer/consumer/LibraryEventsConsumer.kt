package com.sushistack.libconsumer.consumer

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

//@Component
class LibraryEventsConsumer {

    private val log = KotlinLogging.logger {}

    @KafkaListener(topics = ["library-events"], groupId = "library-events-listener-group")
    fun onMessage(consumerRecord: ConsumerRecord<Long, String>) {
        log.info { "ConsumerRecord : $consumerRecord" }
        // produce message by curl call
        // 2024-11-19T13:48:27.345+09:00  INFO 52903 --- [library-events-consumer] [ntainer#0-0-C-1] c.s.l.consumer.LibraryEventsConsumer     : ConsumerRecord : ConsumerRecord(topic = library-events, partition = 2, leaderEpoch = 0, offset = 0, CreateTime = 1731991706959, serialized key size = 8, serialized value size = 128, headers = RecordHeaders(headers = [RecordHeader(key = event-source, value = [115, 99, 97, 110, 110, 101, 114])], isReadOnly = false), key = 0, value = {"libraryEventId":null,"libraryEventType":"NEW","book":{"bookId":456,"bookName":"Kafka Using Spring Boot","bookAuthor":"Dilip"}})
    }
}