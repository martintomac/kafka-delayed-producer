package com.github.martintomac.kafkadelayedproducer.directories

import org.junit.jupiter.api.Test
import kotlin.test.assertTrue

internal class DirectoryReferenceFactoryTest {

    private val directory = SimpleDirectory()
    private val referenceFactory = DirectoryReferenceFactory(directory)

    @Test
    internal fun write() {
        referenceFactory.create("test")

        assertTrue { directory.map[0] == "test" }
    }

    @Test
    internal fun write_multiple() {
        for (i in 0..2) referenceFactory.create("test-$i")

        assertTrue { directory.map.size == 3 }
    }

    @Test
    internal fun read() {
        val reference = referenceFactory.create("test")

        assertTrue { reference.value == "test" }
    }

    @Test
    internal fun read_multiple() {
        val references = (0..2).map { referenceFactory.create("test-$it") }

        for (i in 0..2) assertTrue { references[i].value == "test-$i" }
    }

    @Test
    internal fun remove() {
        val reference = referenceFactory.create("test")

        reference.release()
        assertTrue { directory.map.isEmpty() }
    }

    @Test
    internal fun remove_multiple() {
        val references = (0..2).map { referenceFactory.create("test-$it") }

        for (i in 0..2) references[i].release()
        assertTrue { directory.map.isEmpty() }
    }

    @Test
    internal fun remove_first() {
        val references = (0..2).map { referenceFactory.create("test-$it") }

        references[0].release()
        assertTrue { directory.map.size == 2 }
        for (i in 1..2) assertTrue { references[i].value == "test-$i" }
    }

    private class SimpleDirectory : Directory<Int, String> {

        var nextId = 0
        val map = mutableMapOf<Int, String>()

        override fun write(value: String): Int {
            val id = nextId++
            map[id] = value
            return id
        }

        override fun read(key: Int): String = map[key]!!

        override fun remove(key: Int) {
            map.remove(key)
        }
    }
}