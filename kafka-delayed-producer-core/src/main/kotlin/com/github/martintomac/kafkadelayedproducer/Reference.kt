package com.github.martintomac.kafkadelayedproducer

interface Reference<T : Any> {
    val value: T

    fun release() {
    }
}

interface ReferenceFactory<T : Any> {

    fun create(value: T): Reference<T>

    companion object {
        @Suppress("UNCHECKED_CAST")
        fun <T : Any> instance() = DirectReferenceFactory as ReferenceFactory<T>
    }

    private object DirectReferenceFactory : ReferenceFactory<Any> {
        override fun create(value: Any): Reference<Any> = DirectReference(value)

        private class DirectReference(override val value: Any) : Reference<Any>
    }
}