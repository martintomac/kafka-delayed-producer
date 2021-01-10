package com.github.martintomac.kafkadelayedproducer.directories

interface Directory<K : Any, V : Any> {
    fun write(value: V): K
    fun read(key: K): V
    fun remove(key: K)
}