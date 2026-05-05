package zincflow.processors

import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RecordContent
import kotlin.math.max

/** Extracts one or more field values from a [RecordContent] record
 * and stores them as FlowFile attributes. Mirrors zinc-flow-csharp's
 * `ExtractRecordField` (StdLib/RecordProcessors.cs:370-407).
 * 
 * Config:
 * fields      — semicolon-delimited `"fieldName:attrName"` pairs.
 * Dotted paths supported on the field side
 * (`"address.city:city"`).
 * recordIndex — which record to read (default 0 — the first).
 * 
 * Missing fields and out-of-range indices are silently skipped (the
 * FlowFile passes through) — matches C# semantics. Empty records list
 * also passes through. */
class ExtractRecordField(fields: String, recordIndex: Int) : Processor {
    private val pairs: List<Pair<String, String>> = buildList {
        if (fields.isNotBlank()) {
            for (entry in fields.split(";".toRegex()).dropLastWhile { it.isEmpty() }) {
                val trimmed = entry.trim()
                if (trimmed.isEmpty()) continue
                val colon = trimmed.contains(":")
                require(colon) { "ExtractRecordField: malformed entry '$trimmed' — expected 'fieldName:attrName'" }
                add(Pair(trimmed.substringBefore(":").trim(), trimmed.substringAfter(":").trim()))
            }
        }
    }
    private val recordIndex: Int = max(0, recordIndex)

    override fun process(ff: FlowFile): ProcessorResult {
        val content = ff.content
        if (content !is RecordContent) {
            return ProcessorResult.Failure("ExtractRecordField: expected RecordContent, got " + content.javaClass.getSimpleName(), ff)
        }
        if (content.records.isEmpty() || recordIndex >= content.records.size) {
            return ProcessorResult.Single(ff)
        }
        val record = content.records[recordIndex]
        var result = ff
        for (p in pairs) {
            val value: Any? = resolve(record, p.first)
            if (value != null) {
                result = result.withAttribute(p.second, value.toString())
            }
        }
        return ProcessorResult.Single(result)
    }

    companion object {
        /** Dotted path lookup: `"a.b.c"` traverses
         * record["a"]["b"]["c"]. Returns null on any missing hop or
         * non-map intermediate value. */
        private fun resolve(record: Map<String, Any>, path: String): Any? {
            if (!path.contains(".")) return record[path]
            val parts: Array<String?> = path.split("\\.".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            var current: Any? = record
            for (part in parts) {
                if (current !is MutableMap<*, *>) return null
                current = current[part]
                if (current == null) return null
            }
            return current
        }
    }
}
