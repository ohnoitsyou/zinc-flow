package zincflow.core

import java.util.Map
import java.util.concurrent.atomic.AtomicLong

/** The unit of work flowing through the pipeline. Immutable record — the
 * `withAttribute` / `withContent` helpers produce a new
 * FlowFile rather than mutating. Attribute map is defensively copied on
 * construction. */
class FlowFile(
    @JvmField val id: Long,
    attributes: MutableMap<String?, String?>?,
    @JvmField val content: Content?,
    val timestampMillis: Long,
    @JvmField val hopCount: Int
) {
    /** String form used for logs and debugging only. */
    fun stringId(): String {
        return "ff-" + id
    }

    /** Return a new FlowFile that carries the same id, content, and
     * timestamp but with one attribute added/replaced. */
    fun withAttribute(key: String?, value: String?): FlowFile {
        val next: MutableMap<String?, String?> = HashMap<String?, String?>(attributes)
        next.put(key, value)
        return FlowFile(id, next, content, timestampMillis, hopCount)
    }

    /** Return a new FlowFile with the same metadata but different content. */
    fun withContent(newContent: Content?): FlowFile {
        return FlowFile(id, attributes, newContent, timestampMillis, hopCount)
    }

    /** Return a new FlowFile with hopCount + 1 — called by the executor
     * after each processor dispatch, to detect pipeline loops. */
    fun bumpHop(): FlowFile {
        return FlowFile(id, attributes, content, timestampMillis, hopCount + 1)
    }

    val attributes: MutableMap<String?, String?>?

    init {
        var attributes = attributes
        requireNotNull(attributes) { "attributes must not be null" }
        requireNotNull(content) { "content must not be null" }
        attributes = Map.copyOf<String?, String?>(attributes)
        this.attributes = attributes
    }

    companion object {
        private val ID_SEQ = AtomicLong()

        /** Create a new FlowFile with a fresh sequence id and current-time
         * timestamp, from raw bytes + attribute map. */
        fun create(bytes: ByteArray?, attributes: MutableMap<String?, String?>?): FlowFile {
            return FlowFile(
                ID_SEQ.incrementAndGet(),
                if (attributes == null) Map.of<String?, String?>() else attributes,
                RawContent(bytes),
                System.currentTimeMillis(),
                0
            )
        }

        /** Create a new FlowFile with explicit Content (not necessarily raw). */
        fun create(content: Content?, attributes: MutableMap<String?, String?>?): FlowFile {
            return FlowFile(
                ID_SEQ.incrementAndGet(),
                if (attributes == null) Map.of<String?, String?>() else attributes,
                content,
                System.currentTimeMillis(),
                0
            )
        }
    }
}
