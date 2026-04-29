package zincflow.processors

import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RecordContent
import java.util.List
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
class ExtractRecordField(fields: String?, recordIndex: Int) : Processor {
    @JvmRecord
    private data class Pair(val field: String?, val attr: String?)

    private val pairs: MutableList<Pair>
    private val recordIndex: Int

    init {
        this.recordIndex = max(0, recordIndex)
        val parsed: MutableList<Pair?> = ArrayList<Pair?>()
        if (fields != null && !fields.isBlank()) {
            for (entry in fields.split(";".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()) {
                val trimmed = entry.trim { it <= ' ' }
                if (trimmed.isEmpty()) continue
                val colon = trimmed.indexOf(':')
                require(!(colon <= 0 || colon == trimmed.length - 1)) { "ExtractRecordField: malformed entry '" + trimmed + "' — expected 'fieldName:attrName'" }
                parsed.add(
                    Pair(
                        trimmed.substring(0, colon).trim { it <= ' ' },
                        trimmed.substring(colon + 1).trim { it <= ' ' })
                )
            }
        }
        this.pairs = List.copyOf<Pair?>(parsed)
    }

    override fun process(ff: FlowFile): ProcessorResult {
        if (ff.content !is RecordContent) {
            return ProcessorResult.failure(
                "ExtractRecordField: expected RecordContent, got " + ff.content.javaClass.getSimpleName(), ff
            )
        }
        if (rc.records.isEmpty() || recordIndex >= rc.records.size) {
            return ProcessorResult.single(ff)
        }
        val record: MutableMap<String?, Any?> = rc.records.get(recordIndex)
        var result = ff
        for (p in pairs) {
            val `val`: Any? = Companion.resolve(record, p.field!!)
            if (`val` != null) {
                result = result.withAttribute(p.attr, `val`.toString())
            }
        }
        return ProcessorResult.single(result)
    }

    companion object {
        /** Dotted path lookup: `"a.b.c"` traverses
         * record["a"]["b"]["c"]. Returns null on any missing hop or
         * non-map intermediate value. */
        private fun resolve(record: MutableMap<String?, Any?>, path: String): Any? {
            if (!path.contains(".")) return record.get(path)
            val parts: Array<String?> = path.split("\\.".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            var current: Any? = record
            for (part in parts) {
                if (current !is MutableMap<*, *>) return null
                current = current.get(part)
                if (current == null) return null
            }
            return current
        }
    }
}
