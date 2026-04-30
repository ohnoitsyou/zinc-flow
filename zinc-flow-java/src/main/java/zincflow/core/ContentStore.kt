package zincflow.core

import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream

/** Content-store surface: FlowFile payloads above the claim threshold
 * get offloaded here and referenced by a [ClaimContent]. Keeps
 * large payloads off the heap while still letting them ride through
 * the pipeline as a single value.
 * 
 * Two I/O surfaces — [.retrieve] loads the whole blob
 * into a `byte[]` (convenient for small content) and
 * [.openRead] streams it (required for multi-GB
 * payloads). Implementations must be thread-safe — multiple pipeline
 * threads can store/retrieve concurrently. */
interface ContentStore {
    /** Store a blob; return a stable claim id that can later be used to
     * [.retrieve] the same bytes. */
    fun store(data: ByteArray): String

    /** Stream store — useful for large payloads; reads the input fully
     * without buffering the whole thing in memory. Default delegates
     * to [.store] for implementations that don't have a
     * native streaming path. */
    @Throws(IOException::class)
    fun store(inStream: InputStream): String? = store(inStream.readAllBytes())

    /** Return the bytes previously stored under `claimId`, or an
     * empty array if the claim is unknown (deleted, or never existed). */
    fun retrieve(claimId: String): ByteArray

    /** Stream the claim back. Default wraps [.retrieve];
     * disk-backed stores override to avoid loading the whole blob
     * into memory. */
    @Throws(IOException::class)
    fun openRead(claimId: String): InputStream = ByteArrayInputStream(retrieve(claimId))

    /** Size in bytes of the stored claim, or `-1` when the store
     * doesn't know (e.g. a streaming source that hasn't been counted).
     * Default reads through [.retrieve] and returns its length. */
    fun size(claimId: String): Long = retrieve(claimId).size.toLong()

    /** Remove a claim. No-op if the claim doesn't exist. */
    fun delete(claimId: String)

    fun exists(claimId: String): Boolean
}
