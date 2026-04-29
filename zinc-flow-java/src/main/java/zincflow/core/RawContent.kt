package zincflow.core

/** Raw bytes payload. Ownership follows the FlowFile — processors treat
 * the byte[] as effectively immutable; copy-on-mutate if you need a new
 * payload shape (use [FlowFile.withContent]). */
@JvmRecord
data class RawContent(@JvmField val bytes: ByteArray?) : Content {
    override fun size(): Int {
        return bytes!!.size
    }

    init {
        requireNotNull(bytes) { "RawContent bytes must not be null — use an empty array instead" }
    }
}
