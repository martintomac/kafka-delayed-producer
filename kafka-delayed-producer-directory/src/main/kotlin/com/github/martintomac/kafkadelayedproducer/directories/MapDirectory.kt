package com.github.martintomac.kafkadelayedproducer.directories

class MapDirectory<K : Any, T : Any> private constructor(
    private val keyGenerator: KeyGenerator<K>,
    private val internalMap: MutableMap<K, T>
) : Directory<K, T> {

    companion object {

        fun <T : Any> create(
            mapSupplier: () -> MutableMap<Long, T>
        ): MapDirectory<Long, T> = MapDirectory(
            keyGenerator = LongKeyGenerator(),
            internalMap = mapSupplier()
        )

        fun <K : Any, T : Any> create(
            keyGenerator: KeyGenerator<K>,
            mapSupplier: () -> MutableMap<K, T>
        ): MapDirectory<K, T> = MapDirectory(
            keyGenerator = keyGenerator,
            internalMap = mapSupplier()
        )
    }

    override fun write(value: T): K {
        val key = keyGenerator.next()
        internalMap[key] = value
        return key
    }

    override fun read(key: K): T {
        return internalMap[key] ?: throw NoSuchElementException("Missing value for key: $key")
    }

    override fun remove(key: K) {
        internalMap.remove(key)
    }
}