package com.sushistack.libconsumer.service

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.sushistack.libconsumer.entity.LibraryEvent
import com.sushistack.libconsumer.entity.LibraryEventType
import com.sushistack.libconsumer.repository.LibraryEventsRepository
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.dao.RecoverableDataAccessException
import org.springframework.stereotype.Service


@Service
class LibraryEventsService(
    private val libraryEventsRepository: LibraryEventsRepository,
    private val objectMapper: ObjectMapper
) {

    private val log = KotlinLogging.logger {}

    @Throws(JsonProcessingException::class)
    fun processLibraryEvent(consumerRecord: ConsumerRecord<Long, String>) {
        val libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent::class.java)
        log.info { "libraryEvent : $libraryEvent" }

        if (libraryEvent.libraryEventId != null && (libraryEvent.libraryEventId == 999L)) {
            throw RecoverableDataAccessException("Temporary Network Issue")
        }

        when (libraryEvent.libraryEventType) {
            LibraryEventType.NEW -> save(libraryEvent)
            LibraryEventType.UPDATE -> {
                validate(libraryEvent)
                save(libraryEvent)
            }
        }
    }

    private fun validate(libraryEvent: LibraryEvent) {
        val id = libraryEvent.libraryEventId
        requireNotNull(id) { "Library Event Id is missing" }
        val savedLibraryEvent = libraryEventsRepository.findById(id).orElse(null)
        requireNotNull(savedLibraryEvent) { "Not a valid library Event" }
        log.info { "Validation is successful for the library Event : $savedLibraryEvent" }
    }

    private fun save(libraryEvent: LibraryEvent) {
        libraryEvent.book.libraryEvent = libraryEvent
        libraryEventsRepository.save(libraryEvent)
        log.info { "Successfully Persisted the libary Event $libraryEvent " }
    }

}