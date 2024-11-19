package com.sushistack.libconsumer.entity

import com.fasterxml.jackson.annotation.JsonBackReference
import com.fasterxml.jackson.annotation.JsonIgnore
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.JoinColumn
import jakarta.persistence.OneToOne

@Entity
class Book (
    @Id
    val bookId: Int?,
    val bookName: String,
    val bookAuthor: String,

    @OneToOne
    @JoinColumn(name = "libraryEventId")
    @JsonIgnore
    var libraryEvent: LibraryEvent?
) {
    override fun toString(): String {
        return "Book(bookId=$bookId, bookName=$bookName, bookAuthor=$bookAuthor)"
    }

    constructor(bookId: Int?, bookName: String, bookAuthor: String) : this(bookId, bookName, bookAuthor, null)
}