package zincflow.sources

import zincflow.core.FlowFile
import zincflow.core.FlowFileAttributes
import zincflow.core.PollingSource
import zincflow.core.Source
import zincflow.core.SourcePlugin
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong

/** Timer-driven generator. Emits `batchSize` identical FlowFiles
 * every `pollIntervalMillis`. Handy for load testing, soak
 * tests, heartbeats, and smoke-testing a deployed flow.
 * 
 * Every emitted FlowFile carries:
 * 
 *  * `source` — this source's name
 *  * `generate.index` — monotonically increasing counter
 *  * `http.content.type` — when `contentType` is non-empty
 *  * plus any custom pairs parsed from the `attributes` string
 * 
 * 
 * Config (under `sources.generate`):
 * <pre>
 * sources:
 * generate:
 * content: "ping"
 * contentType: application/json
 * attributes: "env:dev;tenant:acme"
 * batchSize: 1
 * pollIntervalMs: 1000
</pre> * 
 * 
 * Mirrors zinc-flow-csharp's GenerateFlowFile. */
class GenerateFlowFile(
    name: String?, pollIntervalMillis: Long,
    content: String?, contentType: String?,
    attributes: String?, batchSize: Int
) : PollingSource(name, pollIntervalMillis) {
    private val content: ByteArray
    private val baseAttributes: MutableMap<String?, String?>
    private val batchSize: Int
    private val index = AtomicLong()

    init {
        this.content = (if (content == null) "" else content).toByteArray(StandardCharsets.UTF_8)
        this.batchSize = if (batchSize <= 0) 1 else batchSize
        this.baseAttributes = buildAttributes(name, contentType, attributes)
    }

    override fun sourceType(): String {
        return TYPE
    }

    public override fun poll(): MutableList<FlowFile?> {
        val out: MutableList<FlowFile?> = ArrayList<FlowFile?>(batchSize)
        for (i in 0..<batchSize) {
            val attrs: MutableMap<String?, String?> = LinkedHashMap<String?, String?>(baseAttributes)
            attrs.put(FlowFileAttributes.GENERATE_INDEX, index.incrementAndGet().toString())
            out.add(FlowFile.create(content, attrs))
        }
        return out
    }

    /** SPI entry for ServiceLoader discovery. Both the built-in
     * bootstrap and any plugin jar pick up the source through this
     * class, so there's one discovery path across all source sources. */
    class Plugin : SourcePlugin {
        override fun sourceType(): String {
            return TYPE
        }

        override fun description(): String {
            return "Timer-driven FlowFile generator for heartbeats and load tests."
        }

        override fun configKeys(): MutableList<String?> {
            return mutableListOf<String?>("content", "contentType", "attributes", "batchSize", "pollIntervalMs")
        }

        override fun create(name: String?, config: MutableMap<String?, Any?>): Source? {
            val content: String = str(config.get("content"))
            if (content.isEmpty()) return null // disabled when content is absent

            return GenerateFlowFile(
                name,
                longOr(config.get("pollIntervalMs"), 1000),
                content,
                str(config.get("contentType")),
                str(config.get("attributes")),
                longOr(config.get("batchSize"), 1).toInt()
            )
        }

        companion object {
            private fun str(o: Any?): String {
                return if (o == null) "" else o.toString()
            }

            private fun longOr(o: Any?, fallback: Long): Long {
                if (o == null) return fallback
                if (o is Number) return o.toLong()
                try {
                    return o.toString().trim { it <= ' ' }.toLong()
                } catch (ex: NumberFormatException) {
                    return fallback
                }
            }
        }
    }

    companion object {
        const val NAME: String = "generate"
        const val TYPE: String = "GenerateFlowFile"

        private fun buildAttributes(name: String?, contentType: String?, spec: String?): MutableMap<String?, String?> {
            val out: MutableMap<String?, String?> = LinkedHashMap<String?, String?>()
            out.put(FlowFileAttributes.SOURCE, name)
            if (contentType != null && !contentType.isEmpty()) {
                out.put(FlowFileAttributes.HTTP_CONTENT_TYPE, contentType)
            }
            if (spec == null || spec.isBlank()) return out
            // "key:value;key:value" — permissive: ignore entries without a
            // colon instead of throwing, so a minor config typo doesn't
            // crash boot. An empty value is a legal attribute.
            for (pair in spec.split(";".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()) {
                val trimmed = pair.trim { it <= ' ' }
                if (trimmed.isEmpty()) continue
                val idx = trimmed.indexOf(':')
                if (idx <= 0) continue
                out.put(trimmed.substring(0, idx).trim { it <= ' ' }, trimmed.substring(idx + 1).trim { it <= ' ' })
            }
            return out
        }
    }
}
