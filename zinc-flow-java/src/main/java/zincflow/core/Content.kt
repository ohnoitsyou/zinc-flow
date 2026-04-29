package zincflow.core

/** FlowFile payload — a sealed hierarchy so processors can pattern-match
 * on the shape. [RawContent] carries bytes inline;
 * [RecordContent] carries structured records (produced by
 * ConvertJSONToRecord and similar); [ClaimContent] references
 * bytes stored in a [ContentStore]. */
interface Content {
    /** Rough content size — bytes for RawContent, record count for
     * RecordContent. Used for stats + backpressure heuristics. */
    fun size(): Int
}
