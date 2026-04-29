package zincflow.core

/** Lightweight decision helpers for the small-vs-large content split.
 * Small content stays inline as [RawContent]; anything above
 * [.DEFAULT_CLAIM_THRESHOLD] gets offloaded to the wired store
 * and referenced by a [ClaimContent].
 * 
 * Mirror of zinc-flow-csharp's ContentHelpers — the threshold is
 * chosen to keep typical flowfiles (attribute-heavy JSON, small CSV
 * rows, control messages) on the heap fast path while pushing media
 * blobs and multi-MB document payloads out to disk. */
object ContentHelpers {
    /** Default offload threshold: 256 KB. Anything bigger is worth a
     * store round-trip; anything smaller stays inline where the CPU
     * cost of hashing, sharding, and syscalls would outweigh heap
     * pressure. Tunable per call by passing an explicit threshold. */
    @JvmField
    val DEFAULT_CLAIM_THRESHOLD: Int = 256 * 1024

    /** If `data` is under the threshold, wrap it in a
     * [RawContent]; otherwise push it into the store and return
     * a [ClaimContent]. A null store forces the inline path —
     * callers without a store (tests, small pipelines) get the small
     * behavior automatically. */
    @JvmStatic
    fun maybeOffload(store: ContentStore?, data: ByteArray?): Content {
        return maybeOffload(store, data, DEFAULT_CLAIM_THRESHOLD)
    }

    @JvmStatic
    fun maybeOffload(store: ContentStore?, data: ByteArray?, threshold: Int): Content {
        var data = data
        if (data == null) data = ByteArray(0)
        if (store == null || data.size <= threshold) {
            return RawContent(data)
        }
        val claimId = store.store(data)
        return ClaimContent(claimId, data.size)
    }
}
