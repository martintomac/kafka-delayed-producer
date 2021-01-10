package com.github.martintomac.kafkadelayedproducer.directories

import com.github.martintomac.kafkadelayedproducer.Reference
import com.github.martintomac.kafkadelayedproducer.ReferenceFactory

class DirectoryReferenceFactory<T : Any>(directory: Directory<*, T>) : ReferenceFactory<T> {

    @Suppress("UNCHECKED_CAST")
    private val directory = directory as Directory<Any, T>

    override fun create(value: T): Reference<T> = DirectoryReference(value)

    private inner class DirectoryReference(record: T) : Reference<T> {

        private val key: Any = directory.write(record)

        @Suppress("UNCHECKED_CAST")
        override val value: T
            get() = directory.read(key)

        override fun release() = directory.remove(key)
    }
}