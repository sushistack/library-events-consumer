package com.sushistack.libconsumer.config

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.FixedBackOff


@EnableKafka
@Configuration
class LibraryEventsConsumerConfig {

    private var log = KotlinLogging.logger {}

    fun errorHandler(): DefaultErrorHandler = DefaultErrorHandler(FixedBackOff(1000L, 2)).also {
        it.setRetryListeners ({ record, ex, deliveryAttempt ->
            log.info { "Failed Record in Retry Listener, Exception : $ex, deliveryAttempt : $deliveryAttempt" }
        })
    }

    @Bean
    @ConditionalOnMissingBean(name = ["kafkaListenerContainerFactory"])
    fun kafkaListenerContainerFactory(
        configurer: ConcurrentKafkaListenerContainerFactoryConfigurer,
        kafkaConsumerFactory: ConsumerFactory<Any, Any>
    ): ConcurrentKafkaListenerContainerFactory<*, *> {
        val factory = ConcurrentKafkaListenerContainerFactory<Any, Any>()
        configurer.configure(factory, kafkaConsumerFactory)
        // manually ack processing by LibraryEventsConsumerManualOffset
        // factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL


        factory.setConcurrency(3)
        // 2024-11-19T15:24:36.765+09:00  INFO 78698 --- [library-events-consumer] [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : library-events-listener-group: partitions assigned: [library-events-0]
        // 2024-11-19T15:24:36.770+09:00  INFO 78698 --- [library-events-consumer] [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : library-events-listener-group: partitions assigned: [library-events-1]
        // 2024-11-19T15:24:36.774+09:00  INFO 78698 --- [library-events-consumer] [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : library-events-listener-group: partitions assigned: [library-events-2]

        factory.setCommonErrorHandler(errorHandler())
        return factory
    }

}