package com.sushistack.libconsumer.config

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.ContainerProperties


@EnableKafka
@Configuration
class LibraryEventsConsumerConfig {

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
        return factory
    }

}