package com.sushistack.libconsumer.consumer

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.AcknowledgingMessageListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

//@Component
class LibraryEventsConsumerManualOffset: AcknowledgingMessageListener<Long, String> {

    private val log = KotlinLogging.logger {}

    @KafkaListener(topics = ["library-events"], groupId = "library-events-listener-group")
    override fun onMessage(data: ConsumerRecord<Long, String>, acknowledgment: Acknowledgment?) {
        log.info { "ConsumerRecord : $data" }
        acknowledgment?.acknowledge()
    }
}