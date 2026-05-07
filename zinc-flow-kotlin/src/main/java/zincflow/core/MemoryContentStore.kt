package zincflow.core

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/** In-process [ContentStore] — holds every claim in a
 * [ConcurrentHashMap]. Used by tests and by production pipelines
 * that don't need disk-backed content (the bytes already live in the
 * JVM anyway). */
class MemoryContentStore(initialData: Map<String, ByteArray> = mapOf()) : ContentStore {
    private val content = initialData.toMutableMap()
    private val counter = AtomicLong(content.size.toLong())

    override fun store(data: ByteArray): String {
        val bytes: ByteArray = data
        val id = "mem-claim-" + counter.incrementAndGet()
        content[id] = bytes
        return id
    }

    override fun retrieve(claimId: String): ByteArray {
        return content[claimId] ?: ByteArray(0)
    }

    override fun delete(claimId: String) {
        content.remove(claimId)
    }

    override fun exists(claimId: String): Boolean {
        return content.containsKey(claimId)
    }

    fun size(): Int {
        return content.size
    }
    companion object {
        const val NAME = "MemoryContentStore"
    }
}
