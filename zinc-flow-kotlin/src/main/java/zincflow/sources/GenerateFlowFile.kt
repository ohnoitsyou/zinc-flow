package zincflow.sources

import zincflow.core.FlowFile
import zincflow.core.FlowFileAttributes
import zincflow.core.PollingSource
import zincflow.core.Source
import zincflow.core.SourcePlugin
import zincflow.toIntOrDefault
import zincflow.toLongOrDefault
import zincflow.toStringOrDefault
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
class GenerateFlowFile @JvmOverloads constructor(
    name: String,
    pollIntervalMillis: Long,
    content: String?,
    contentType: String?,
    attributesAsString: String = "",
    batchSize: Int = 1,
) : PollingSource(name, pollIntervalMillis) {
    private val content: ByteArray = (content ?: "").toByteArray(StandardCharsets.UTF_8)
    private val baseAttributes: Map<String, String> = buildAttributes(name, contentType ?: "", attributesAsString)
    private val index = AtomicLong()
    private val batchSize = batchSize.coerceAtLeast(1)

    override fun sourceType(): String = TYPE

    override fun poll(): MutableList<FlowFile> {
        return buildList {
            repeat(batchSize) {
                val attrs = baseAttributes.toMutableMap()
                attrs[FlowFileAttributes.GENERATE_INDEX] = index.incrementAndGet().toString()
                add(FlowFile.create(content, attrs))
            }
        }.toMutableList()
    }

    /** SPI entry for ServiceLoader discovery. Both the built-in
     * bootstrap and any plugin jar pick up the source through this
     * class, so there's one discovery path across all source sources. */
    class Plugin : SourcePlugin {
        override fun sourceType(): String = TYPE

        override fun description(): String =
            "Timer-driven FlowFile generator for heartbeats and load tests."

        override fun configKeys(): MutableList<String> =
            mutableListOf("content", "contentType", "attributes", "batchSize", "pollIntervalMs")

        override fun create(name: String, config: Map<String, Any>): Source? {
            val content = config["content"] as? String ?: ""
            if (content.isEmpty()) return null // disabled when content is absent

            return GenerateFlowFile(
                name,
                config["pollIntervalMs"].toLongOrDefault(1000L),
                content,
                config["contentType"].toStringOrDefault(""),
                config["attributes"].toStringOrDefault(""),
                config["batchSize"].toIntOrDefault(1),
            )
        }
    }


    companion object {
        const val NAME: String = "generate"
        const val TYPE: String = "GenerateFlowFile"

        private const val ATTRIBUTE_SEPARATOR = ";"
        private const val KEY_VALUE_SEPARATOR = ":"

        private fun buildAttributes(name: String, contentType: String, attributeString: String): Map<String, String> {
            return buildMap {
                put(FlowFileAttributes.SOURCE, name)
                if(contentType.isNotEmpty()) put(FlowFileAttributes.HTTP_CONTENT_TYPE, contentType)
                if(attributeString.isNotBlank()) {
                    for (pair in attributeString.split(ATTRIBUTE_SEPARATOR).dropLastWhile { it.isEmpty() }) {
                        val trimmed = pair.trim()
                        if (trimmed.isEmpty() || !trimmed.contains(KEY_VALUE_SEPARATOR)) continue
                        put(trimmed.substringBefore(KEY_VALUE_SEPARATOR).trim(), trimmed.substringAfter(KEY_VALUE_SEPARATOR).trim())
                    }
                }
            }
        }
    }
}
