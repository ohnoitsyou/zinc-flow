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
    private val removeMode: Boolean
    private val attributeSet: MutableSet<String?>

    init {
        this.removeMode = !"keep".equals(mode, ignoreCase = true)
        this.attributeSet = if (attributes == null || attributes.isBlank()) mutableSetOf<String?>() else
            Arrays.stream<String>(attributes.split(";".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
                .map<String?> { obj: String? -> obj!!.trim { it <= ' ' } }
                .filter { s: String? -> !s!!.isEmpty() }
                .collect(Collectors.toUnmodifiableSet())
    }

    override fun process(ff: FlowFile): ProcessorResult {
        val filtered: MutableMap<String?, String?> = LinkedHashMap<String?, String?>()
        for (entry in ff.attributes.entries) {
            val listed = attributeSet.contains(entry.key)
            if (if (removeMode) !listed else listed) {
                filtered.put(entry.key, entry.value)
            }
        }
        return ProcessorResult.single(
            FlowFile(
                ff.id, filtered, ff.content, ff.timestampMillis, ff.hopCount
            )
        )
    }
}
