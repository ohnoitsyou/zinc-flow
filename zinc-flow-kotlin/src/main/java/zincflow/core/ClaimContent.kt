package zincflow.core

/** Content handle that points at a [ContentStore] rather than
 * holding bytes inline. Processors that need the bytes resolve the
 * claim via [ContentResolver.resolve];
 * processors that only need to move the FlowFile through the graph
 * don't need to look at the content at all.
 * 
 * `size` is recorded at claim time so stats and backpressure
 * heuristics don't need to round-trip to the store. */
@JvmRecord
data class ClaimContent(@JvmField val claimId: String, @JvmField val size: Int) : Content {
    override fun size(): Int {
        return size
    }

    init {
        require(claimId.isNotEmpty()) { "ClaimContent claimId must not be blank" }
        require(size >= 0) { "ClaimContent size must not be negative" }
    }
}
