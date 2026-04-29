package zincflow.processors

import org.apache.avro.Schema
import org.apache.commons.jexl3.JexlBuilder
import org.apache.commons.jexl3.JexlEngine
import org.apache.commons.jexl3.JexlException
import org.apache.commons.jexl3.JexlExpression
import org.apache.commons.jexl3.MapContext
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RecordContent
import zincflow.core.SchemaDefs
import java.util.List
import java.util.Locale
import java.util.Set
import java.util.StringJoiner
import kotlin.Any
import kotlin.Boolean
import kotlin.ByteArray
import kotlin.Double
import kotlin.Float
import kotlin.IllegalArgumentException
import kotlin.Int
import kotlin.Long
import kotlin.collections.ArrayList
import kotlin.collections.HashSet
import kotlin.collections.LinkedHashMap
import kotlin.collections.MutableList
import kotlin.collections.MutableMap
import kotlin.collections.MutableSet
import kotlin.collections.contains
import kotlin.collections.containsKey
import kotlin.collections.dropLastWhile
import kotlin.collections.get
import kotlin.collections.isEmpty
import kotlin.collections.mutableListOf
import kotlin.collections.remove
import kotlin.collections.toTypedArray
import kotlin.plus
import kotlin.require
import kotlin.text.isBlank
import kotlin.text.isEmpty
import kotlin.text.lowercase
import kotlin.text.split
import kotlin.text.toRegex
import kotlin.text.trim
import kotlin.text.uppercase

/** Applies a semicolon-delimited operations palette to every record in a
 * RecordContent payload. Mirrors zinc-flow-csharp's
 * `TransformRecord` (StdLib/ExpressionProcessors.cs:177-333).
 * 
 * Operations:
 * 
 *  * `rename:oldName:newName`     — rename a field
 *  * `remove:fieldName`           — remove a field
 *  * `add:fieldName:value`        — add a string-typed field with literal value
 *  * `copy:source:target`         — copy field value (preserves type)
 *  * `toUpper:fieldName`          — uppercase a string field
 *  * `toLower:fieldName`          — lowercase a string field
 *  * `default:fieldName:value`    — set field to string value if missing/null
 *  * `compute:targetField:expr`   — evaluate JEXL expression, assign result
 * 
 * 
 * `compute` uses Apache Commons JEXL (arithmetic, booleans, ternary,
 * string ops); C#'s track currently uses a lighter template DSL and is
 * tracked to catch up in the post-Java cohort. Every other op is bit-for-bit
 * portable across tracks. */
class TransformRecord(operationsSpec: String) : Processor {
    @JvmRecord
    private data class Directive(val op: String?, val arg1: String?, val arg2: String?, val compiled: JexlExpression?)

    private val directives: MutableList<Directive>

    init {
        require(!(operationsSpec == null || operationsSpec.isBlank())) { "TransformRecord: operations must have at least one directive" }
        val parsed: MutableList<Directive?> = ArrayList<Directive?>()
        for (raw in operationsSpec.split(";".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()) {
            val directive = raw.trim { it <= ' ' }
            if (directive.isEmpty()) continue
            // Split into up to 3 parts so arg2 can legally contain colons
            // (e.g. compute expressions with dotted paths).
            val parts = directive.split(":".toRegex(), limit = 3).toTypedArray()
            require(parts.size >= 2) {
                ("TransformRecord: malformed directive '" + directive
                        + "' — expected 'op:arg1' or 'op:arg1:arg2'")
            }
            val op: String? = parts[0]
            require(KNOWN_OPS.contains(op)) {
                ("TransformRecord: unknown op '" + op + "' in directive '" + directive
                        + "' — valid: " + String.join(", ", KNOWN_OPS))
            }
            val arg1: kotlin.String? = parts[1]
            val arg2 = if (parts.size > 2) parts[2] else ""
            var compiled: JexlExpression? = null
            if ("compute" == op) {
                require(!arg2.isBlank()) { "TransformRecord: compute requires an expression — 'compute:target:expr'" }
                try {
                    compiled = JEXL.createExpression(arg2)
                } catch (ex: JexlException) {
                    throw IllegalArgumentException(
                        "TransformRecord: invalid JEXL in compute '" + arg2 + "' — " + ex.message, ex
                    )
                }
            }
            parsed.add(Directive(op, arg1, arg2, compiled))
        }
        require(!parsed.isEmpty()) { "TransformRecord: operations spec produced no directives" }
        this.directives = List.copyOf<Directive?>(parsed)
    }

    override fun process(ff: FlowFile): ProcessorResult {
        if (ff.content !is RecordContent) {
            return ProcessorResult.failure(
                "TransformRecord: expected RecordContent, got " + ff.content.javaClass.getSimpleName(), ff
            )
        }
        if (rc.records.isEmpty()) {
            return ProcessorResult.single(ff)
        }

        val transformed: MutableList<MutableMap<kotlin.String?, Any?>?> =
            ArrayList<MutableMap<kotlin.String?, Any?>?>(rc.records.size)
        for (record in rc.records) {
            val dict: MutableMap<kotlin.String?, Any?> = LinkedHashMap<kotlin.String?, Any?>(record)
            for (d in directives) {
                applyOp(dict, d)
            }
            transformed.add(dict)
        }

        // Re-infer schema from first record post-transform. Unlike C# we
        // don't track per-field original Avro types — the safest choice
        // is a fresh inferred schema, which covers the common cases
        // (add/compute adds typed fields; rename/copy preserves types
        // through the map; remove drops fields cleanly).
        val schemaName: kotlin.String? = if (rc.schema == null) "TransformedRecord" else rc.schema.getName()
        val outSchema: Schema? = inferSchema(schemaName, transformed.getFirst())

        return ProcessorResult.single(ff.withContent(RecordContent(transformed, outSchema)))
    }

    private fun applyOp(dict: MutableMap<kotlin.String?, Any?>, d: Directive) {
        when (d.op) {
            "rename" -> {
                if (dict.containsKey(d.arg1)) dict.put(d.arg2, dict.remove(d.arg1))
            }

            "remove" -> dict.remove(d.arg1)
            "add" -> dict.put(d.arg1, d.arg2)
            "copy" -> {
                if (dict.containsKey(d.arg1)) dict.put(d.arg2, dict.get(d.arg1))
            }

            "toUpper" -> {
                if (dict.get(d.arg1) is kotlin.String) dict.put(d.arg1, s.uppercase(Locale.getDefault()))
            }

            "toLower" -> {
                if (dict.get(d.arg1) is kotlin.String) dict.put(d.arg1, s.lowercase(Locale.getDefault()))
            }

            "default" -> {
                if (!dict.containsKey(d.arg1) || dict.get(d.arg1) == null) dict.put(d.arg1, d.arg2)
            }

            "compute" -> {
                val ctx = MapContext()
                ctx.set("record", dict)
                for (e in dict.entries) ctx.set(e.key, e.value)
                try {
                    dict.put(d.arg1, d.compiled!!.evaluate(ctx))
                } catch (ex: JexlException) {
                    // C# silently skips failed computes — match that so
                    // one bad record doesn't crash the whole batch.
                }
            }

            else -> {}
        }
    }

    companion object {
        private val JEXL: JexlEngine = JexlBuilder()
            .strict(false).safe(true).silent(false).create()

        private val KNOWN_OPS: MutableSet<kotlin.String?> = Set.copyOf<kotlin.String?>(
            mutableListOf<kotlin.String?>(
                "rename", "remove", "add", "copy", "toUpper", "toLower", "default", "compute"
            )
        )

        /** Build a flat Avro Schema from a record's keys + value types.
         * Matches the inference logic in ConvertJSONToRecord / ConvertCSVToRecord. */
        private fun inferSchema(name: kotlin.String?, record: MutableMap<kotlin.String?, Any?>): Schema? {
            if (record.isEmpty()) return null
            val defs = StringJoiner(",")
            val seen: MutableSet<kotlin.String?> = HashSet<kotlin.String?>()
            for (entry in record.entries) {
                if (!seen.add(entry.key)) continue
                defs.add(entry.key + ":" + inferType(entry.value))
            }
            return SchemaDefs.parse(name, defs.toString())
        }

        private fun inferType(v: Any?): kotlin.String {
            if (v == null) return "string"
            if (v is Boolean) return "boolean"
            if (v is Int) return "int"
            if (v is Long) return "long"
            if (v is Float) return "float"
            if (v is Double) return "double"
            if (v is ByteArray) return "bytes"
            return "string"
        }
    }
}
