package com.sushistack.libconsumer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class LibraryEventsConsumerApplication

fun main(args: Array<String>) {
    runApplication<LibraryEventsConsumerApplication>(*args)
}
