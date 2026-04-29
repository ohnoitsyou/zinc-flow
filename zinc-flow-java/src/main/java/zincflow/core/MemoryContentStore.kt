package zincflow.core

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/** In-process [ContentStore] — holds every claim in a
 * [ConcurrentHashMap]. Used by tests and by production pipelines
 * that don't need disk-backed content (the bytes already live in the
 * JVM anyway). */
class MemoryContentStore : ContentStore {
    private val data = ConcurrentHashMap<String?, ByteArray?>()
    private val counter = AtomicLong()

    override fun store(bytes: ByteArray?): String {
        var bytes = bytes
        if (bytes == null) bytes = ByteArray(0)
        val id = "mem-claim-" + counter.incrementAndGet()
        data.put(id, bytes)
        return id
    }

    override fun retrieve(claimId: String?): ByteArray {
        val v = if (claimId == null) null else data.get(claimId)
        return if (v == null) ByteArray(0) else v
    }

    override fun delete(claimId: String?) {
        if (claimId != null) data.remove(claimId)
    }

    override fun exists(claimId: String?): Boolean {
        return claimId != null && data.containsKey(claimId)
    }

    fun size(): Int {
        return data.size
    }
}
