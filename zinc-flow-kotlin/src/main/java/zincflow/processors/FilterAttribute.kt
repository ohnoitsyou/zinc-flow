package zincflow.processors

import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import java.util.Arrays
import java.util.stream.Collectors

/** Keeps or removes a named set of attributes from the FlowFile, always
 * passing the FlowFile through. Mirrors zinc-flow-csharp's
 * `FilterAttribute` (StdLib/Processors.cs:62-90).
 * 
 * Config:
 * mode       — "remove" (default) or "keep"
 * attributes — semicolon-separated attribute names */
class FilterAttribute(mode: String?, attributes: String?) : Processor {
    private val removeMode: Boolean = !"keep".equals(mode, ignoreCase = true)
    private val attributeSet: Set<String> = if (attributes.isNullOrBlank()) setOf() else
        attributes.split(";")
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .toSet()

    override fun process(ff: FlowFile): ProcessorResult {
        val filtered = ff.attributes.entries.mapNotNull { (key, value) ->
            val listed = attributeSet.contains(key)
            if (if (removeMode) !listed else listed) {
                key to value
            } else {
                null
            }
        }.toMap()
        return ProcessorResult.Single(FlowFile(ff.id, filtered, ff.content, ff.timestampMillis, ff.hopCount))
    }
}
