package zincflow.core

/** Resolve any [Content] variant to a raw byte[] so processors
 * that consume content can stay variant-agnostic. RawContent returns
 * its bytes directly; ClaimContent fetches from the store;
 * RecordContent has no byte form and returns an error.
 * 
 * The tuple-style return (bytes + error string) mirrors the C#
 * `ContentHelpers.Resolve`; empty error means success. */
object ContentResolver {
    fun resolve(content: Content, store: ContentStore?): Resolution {
        return when(content) {
            is RawContent -> Resolution(content.bytes)
            is ClaimContent if store == null -> Resolution(ByteArray(0), "ClaimContent requires a ContentStore but none was provided")
            is ClaimContent -> Resolution(store!!.retrieve(content.claimId)) // Previous branch covers the store is null case
            else -> Resolution(ByteArray(0), "unknown content variant: ${content.javaClass.getName()}")
        }
    }

    data class Resolution(val bytes: ByteArray, val error: String? = null) {
        fun ok(): Boolean = error.isNullOrEmpty()

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Resolution

            if (!bytes.contentEquals(other.bytes)) return false
            if (error != other.error) return false

            return true
        }

        override fun hashCode(): Int {
            var result = bytes.contentHashCode()
            result = 31 * result + (error?.hashCode() ?: 0)
            return result
        }
    }
}
