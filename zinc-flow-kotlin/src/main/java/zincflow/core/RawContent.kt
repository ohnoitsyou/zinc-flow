package zincflow.core

/** Raw bytes payload. Ownership follows the FlowFile — processors treat
 * the byte[] as effectively immutable; copy-on-mutate if you need a new
 * payload shape (use [FlowFile.withContent]). */
data class RawContent(val bytes: ByteArray) : Content {
    override fun size(): Int {
        return bytes.size
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as RawContent

        return bytes.contentEquals(other.bytes)
    }

    override fun hashCode(): Int {
        return bytes.contentHashCode()
    }
}
