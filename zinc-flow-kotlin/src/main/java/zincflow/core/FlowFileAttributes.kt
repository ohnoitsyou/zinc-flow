package zincflow.core

/** Conventional FlowFile attribute names. Not exhaustive — processors
 * are free to set anything — but every attribute that appears in more
 * than one built-in processor or test lives here so a rename ripples
 * in one place.
 * 
 * Grouped by producer for easier navigation. */
object FlowFileAttributes {
    // --- Common (set by sources) -----------------------------------------
    /** Filename of the on-disk source, no path. */
    const val FILENAME: String = "filename"

    /** Absolute path of the source file. */
    const val PATH: String = "path"

    /** Raw byte size of the source payload. */
    const val SIZE: String = "size"

    /** Identifier of the source that emitted the FlowFile. */
    const val SOURCE: String = "source"

    // --- FlowFile V3 framing ---------------------------------------------
    /** Zero-based index of the frame within a V3 bundle. */
    const val V3_FRAME_INDEX: String = "v3.frame.index"

    /** Total frame count in the V3 bundle. */
    const val V3_FRAME_COUNT: String = "v3.frame.count"

    // --- PutHTTP outcomes -------------------------------------------------
    /** HTTP status code from the last PutHTTP call. */
    const val PUTHTTP_STATUS: String = "puthttp.status"

    /** Byte size of the response body. */
    const val PUTHTTP_RESPONSE_SIZE: String = "puthttp.response.size"

    // --- GenerateFlowFile -------------------------------------------------
    /** Monotonic counter emitted by GenerateFlowFile. */
    const val GENERATE_INDEX: String = "generate.index"

    /** Optional content type set by GenerateFlowFile / HTTP ingress. */
    const val HTTP_CONTENT_TYPE: String = "http.content.type"
}
