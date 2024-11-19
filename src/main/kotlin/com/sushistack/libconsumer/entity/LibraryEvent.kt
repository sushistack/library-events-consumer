package com.sushistack.libconsumer.entity

import jakarta.persistence.*

@Entity
class LibraryEvent(
    @Id
    @GeneratedValue
    val libraryEventId: Long?,
    @Enumerated(EnumType.STRING)
    val libraryEventType: LibraryEventType,
    @OneToOne(mappedBy = "libraryEvent", cascade = [CascadeType.ALL])
    val book: Book
) {
    override fun toString(): String {
        return "LibraryEvent(libraryEventId=$libraryEventId, libraryEventType=$libraryEventType)"
    }
}