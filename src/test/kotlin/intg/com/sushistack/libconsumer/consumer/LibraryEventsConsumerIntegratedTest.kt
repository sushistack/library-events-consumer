package com.sushistack.libconsumer.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.ninjasquad.springmockk.SpykBean
import com.sushistack.libconsumer.entity.Book
import com.sushistack.libconsumer.entity.LibraryEvent
import com.sushistack.libconsumer.entity.LibraryEventType
import com.sushistack.libconsumer.repository.LibraryEventsRepository
import com.sushistack.libconsumer.service.LibraryEventsService
import io.mockk.verify
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.ContainerTestUtils
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.context.TestPropertySource
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.stream.Collectors


@SpringBootTest
@EmbeddedKafka(
    topics = ["library-events"],
    partitions = 3
)
@TestPropertySource(properties = [
    "spring.kafka.producer.bootstrap-servers=\${spring.embedded.kafka.brokers}",
    "spring.kafka.consumer.bootstrap-servers=\${spring.embedded.kafka.brokers}"
])
class LibraryEventsConsumerIntegratedTest {

    @Autowired
    lateinit var embeddedKafkaBroker: EmbeddedKafkaBroker

    @Autowired
    lateinit var kafkaTemplate: KafkaTemplate<Long?, String>

    @Autowired
    lateinit var endpointRegistry: KafkaListenerEndpointRegistry

    @SpykBean
    lateinit var libraryEventsConsumerSpy: LibraryEventsConsumer

    @SpykBean
    lateinit var libraryEventsServiceSpy: LibraryEventsService

    @Autowired
    lateinit var libraryEventsRepository: LibraryEventsRepository

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @BeforeEach
    fun setup() {
        endpointRegistry.listenerContainers.forEach { ContainerTestUtils.waitForAssignment(it, embeddedKafkaBroker.partitionsPerTopic) }
    }

    @Test
    fun publishNewLibraryEvent() {
        // Given
        val json = """{"libraryEventId":null,"libraryEventType": "NEW","book":{"bookId":456,"bookName":"Kafka Using Spring Boot","bookAuthor":"Dilip"}}"""
        kafkaTemplate.sendDefault(json).get()

        // When
        // 비동기 작업이 완료될 때까지 테스트 스레드가 기다리도록
        val latch = CountDownLatch(1)
        latch.await(3, TimeUnit.SECONDS)

        // Then
        verify(exactly = 1) { libraryEventsConsumerSpy.onMessage(any<ConsumerRecord<Long, String>>()) }
        verify(exactly = 1) { libraryEventsServiceSpy.processLibraryEvent(any<ConsumerRecord<Long, String>>()) }

        val libraryEvents = libraryEventsRepository.findAll()
        Assertions.assertThat(libraryEvents).hasSize(1)

        Assertions.assertThat(libraryEvents)
            .allSatisfy { libraryEvent ->
                Assertions.assertThat(libraryEvent.libraryEventId).isNotNull()
                Assertions.assertThat(libraryEvent.book.bookId).isEqualTo(456)
            }

    }

    @Test
    fun publishUpdateLibraryEvent() {
        // Given
        val json = """{"libraryEventId":null,"libraryEventType": "NEW","book":{"bookId":456,"bookName":"Kafka Using Spring Boot","bookAuthor":"Dilip"}}"""
        val libraryEvent = objectMapper.readValue(json, LibraryEvent::class.java)
        libraryEvent.book.libraryEvent = libraryEvent
        libraryEventsRepository.save(libraryEvent)

        val updatedBook = Book(bookId = 456, bookName = "Kafka Using Spring Boot 2.x", bookAuthor = "Dilip")
        libraryEvent.libraryEventType = LibraryEventType.UPDATE
        libraryEvent.book = updatedBook
        val updatedJson = objectMapper.writeValueAsString(libraryEvent)
        kafkaTemplate.sendDefault(libraryEvent.libraryEventId!!, updatedJson).get()

        // When
        val latch = CountDownLatch(1)
        latch.await(3, TimeUnit.SECONDS)

        // Then
        verify(exactly = 1) { libraryEventsConsumerSpy.onMessage(any<ConsumerRecord<Long, String>>()) }
        verify(exactly = 1) { libraryEventsServiceSpy.processLibraryEvent(any<ConsumerRecord<Long, String>>()) }

        val persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.libraryEventId!!).get()
        Assertions.assertThat(persistedLibraryEvent.book.bookName).isEqualTo("Kafka Using Spring Boot 2.x")
    }

    @Test
    fun publishUpdateLibraryEventNullLibraryEvent() {
        // Given
        val json = """{"libraryEventId":null,"libraryEventType": "UPDATE","book":{"bookId":456,"bookName":"Kafka Using Spring Boot","bookAuthor":"Dilip"}}"""
        kafkaTemplate.sendDefault(json).get()

        // When
        val latch = CountDownLatch(1)
        latch.await(5, TimeUnit.SECONDS)

        // Then
        // null 이기 때문에 실패 하고 10번의 시도를 진행
        // maxAttemps = 2 로 설정
        verify(exactly = 3) { libraryEventsConsumerSpy.onMessage(any<ConsumerRecord<Long, String>>()) }
        verify(exactly = 3) { libraryEventsServiceSpy.processLibraryEvent(any<ConsumerRecord<Long, String>>()) }
    }
}