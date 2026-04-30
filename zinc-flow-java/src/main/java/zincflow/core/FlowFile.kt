package zincflow.core

import java.util.concurrent.atomic.AtomicLong
import kotlin.collections.toMap

/** The unit of work flowing through the pipeline. Immutable record — the
 * `withAttribute` / `withContent` helpers produce a new
 * FlowFile rather than mutating. Attribute map is defensively copied on
 * construction. */
data class FlowFile private constructor(
    val id: Long,
    val attributes: Map<String, String>,
    val content: Content,
    val timestampMillis: Long,
    val hopCount: Int
) {
    /** String form used for logs and debugging only. */
    fun stringId(): String = "ff-$id"

    /** Return a new FlowFile that carries the same id, content, and
     * timestamp but with one attribute added/replaced. */
    fun withAttribute(key: String, value: String): FlowFile {
        return this.copy(attributes = (attributes + (key to value)))
    }

    /** Return a new FlowFile with the same metadata but different content. */
    fun withContent(newContent: Content): FlowFile {
        return this.copy(content = newContent)
    }

    /** Return a new FlowFile with hopCount + 1 — called by the executor
     * after each processor dispatch, to detect pipeline loops. */
    fun bumpHop(): FlowFile {
        return FlowFile(id, attributes, content, timestampMillis, hopCount + 1)
    }

    companion object {
        operator fun invoke(
            id: Long,
            attributes: Map<String, String>,
            content: Content,
            timestampMillis: Long,
            hopCount: Int
        ): FlowFile {
            return FlowFile(id, attributes.toMap(), content, timestampMillis, hopCount)
        }

        private val ID_SEQ = AtomicLong()

        /** Create a new FlowFile with a fresh sequence id and current-time
         * timestamp, from raw bytes + attribute map. */
        fun create(bytes: ByteArray, attributes: MutableMap<String, String> = mutableMapOf()): FlowFile {
            return FlowFile(
                ID_SEQ.incrementAndGet(),
                attributes,
                RawContent(bytes),
                System.currentTimeMillis(),
                0
            )
        }

        /** Create a new FlowFile with explicit Content (not necessarily raw). */
        fun create(content: Content, attributes: MutableMap<String, String>): FlowFile {
            return FlowFile(
                ID_SEQ.incrementAndGet(),
                attributes,
                content,
                System.currentTimeMillis(),
                0
            )
        }
    }
}
