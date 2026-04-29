package zincflow.core

/** Resolve any [Content] variant to a raw byte[] so processors
 * that consume content can stay variant-agnostic. RawContent returns
 * its bytes directly; ClaimContent fetches from the store;
 * RecordContent has no byte form and returns an error.
 * 
 * The tuple-style return (bytes + error string) mirrors the C#
 * `ContentHelpers.Resolve`; empty error means success. */
object ContentResolver {
    @JvmStatic
    fun resolve(content: Content, store: ContentStore?): Resolution {
        if (content is RawContent) {
            return Resolution(content.bytes, "")
        }
        if (content is ClaimContent) {
            if (store == null) {
                return Resolution(ByteArray(0), "ClaimContent requires a ContentStore but none was provided")
            }
            val out = store.retrieve(content.claimId)
            return Resolution(out, "")
        }
        if (content is RecordContent) {
            return Resolution(
                ByteArray(0),
                "cannot resolve RecordContent to raw bytes — serialize with a record writer first"
            )
        }
        return Resolution(ByteArray(0), "unknown content variant: " + content.javaClass.getName())
    }

    @JvmRecord
    data class Resolution(@JvmField val bytes: ByteArray?, val error: String?) {
        fun ok(): Boolean {
            return error == null || error.isEmpty()
        }
    }
}
