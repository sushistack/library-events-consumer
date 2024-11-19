package com.sushistack.libconsumer.repository

import com.sushistack.libconsumer.entity.LibraryEvent
import org.springframework.data.repository.CrudRepository

interface LibraryEventsRepository: CrudRepository<LibraryEvent, Long> {
}