package com.github.martintomac.kafkadelayedproducer.directories

import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

internal class MapDirectoryTest {

    private val directoryMap = ConcurrentHashMap<Long, String>()
    private val directory = MapDirectory.create(LongKeyGenerator(0)) { directoryMap }

    @Test
    internal fun write() {
        val key = directory.write("test")

        assertTrue { key == 0L }
        assertTrue { directoryMap.size == 1 }
        assertTrue { directoryMap[0] == "test" }
    }

    @Test
    internal fun write_multiple() {
        val keys = (0..2).map { directory.write("test-$it") }

        assertEquals(actual = keys, expected = (0..2L).toList())
        assertTrue { directoryMap.size == 3 }
        for (i in 0..2L) assertTrue { directoryMap[i] == "test-$i" }
    }

    @Test
    internal fun read_missing_key() {
        assertFailsWith(NoSuchElementException::class) { directory.read(0) }
    }

    @Test
    internal fun read() {
        val key = directory.write("test")

        assertTrue { directory.read(key) == "test" }
    }

    @Test
    internal fun read_multiple() {
        val keys = (0..2).map { directory.write("test-$it") }

        for (i in 0..2) assertTrue { directory.read(keys[i]) == "test-$i" }
    }

    @Test
    internal fun remove() {
        val key = directory.write("test")
        directory.remove(key)

        assertTrue { directoryMap.isEmpty() }
    }

    @Test
    internal fun remove_multiple() {
        val keys = (0..2).map { directory.write("test-$it") }
        for (key in keys) directory.remove(key)

        assertTrue { directoryMap.isEmpty() }
    }

    @Test
    internal fun remove_first() {
        val keys = (0..2).map { directory.write("test-$it") }
        directory.remove(keys[0])

        assertTrue { directoryMap.size == 2 }
        for (i in 1..2) assertTrue { directoryMap[keys[i]] == "test-$i" }
    }
}