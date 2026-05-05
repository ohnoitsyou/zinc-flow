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
import java.util.Locale
import java.util.StringJoiner
import kotlin.Any
import kotlin.Boolean
import kotlin.ByteArray
import kotlin.Double
import kotlin.Float
import kotlin.IllegalArgumentException
import kotlin.Int
import kotlin.Long
import kotlin.collections.HashSet
import kotlin.collections.LinkedHashMap
import kotlin.collections.MutableMap
import kotlin.collections.MutableSet
import kotlin.collections.mutableListOf
import kotlin.plus
import kotlin.require
import kotlin.text.lowercase
import kotlin.text.split
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
    private data class Directive(val op: String, val target: String, val expression: String, val compiled: JexlExpression?)

    private val directives: List<Directive> = operationsSpec.parseSpecToOperations()

    private fun String.parseSpecToOperations() : List<Directive> {
        require(isNotBlank()) { "TransformRecord: operations must have at least one directive" }
        val parsed = mutableListOf<Directive>()
        for (directive in split(";").mapNotNull { op -> op.trim().takeIf { it.isNotEmpty() }}) {
            // Split into up to 3 parts so arg2 can legally contain colons
            // (e.g. compute expressions with dotted paths).
            val parts = directive.split(":", limit = 3)
            require(parts.size >= 2) {
                "TransformRecord: malformed directive '$directive' — expected 'operation:target' or 'operation:target:expression'"
            }

            val (op, target, expression) = parts
            require(KNOWN_OPS.contains(op)) {
                "TransformRecord: unknown op '$op' in directive '$directive' — valid: ${KNOWN_OPS.joinToString()}"
            }
            var compiled: JexlExpression? = null
            if ("compute" == op) {
                require(expression.isNotBlank()) { "TransformRecord: compute requires an expression — 'operation:target:expr'" }

                try {
                    compiled = JEXL.createExpression(expression)
                } catch (ex: JexlException) {
                    throw IllegalArgumentException("TransformRecord: invalid JEXL in compute '$expression' — ${ex.message}", ex)
                }
            }
            parsed.add(Directive(op, target, expression, compiled))
        }
        require(parsed.isNotEmpty()) { "TransformRecord: operations spec produced no directives" }
        return parsed
    }

    override fun process(ff: FlowFile): ProcessorResult {
        val content = ff.content
        if (content !is RecordContent) {
            return ProcessorResult.Failure(
                "TransformRecord: expected RecordContent, got " + ff.content.javaClass.getSimpleName(), ff
            )
        }
        if (content.records.isEmpty()) {
            return ProcessorResult.Single(ff)
        }

        val transformed = mutableListOf<MutableMap<String, Any>>()
        for (record in content.records) {
            val dict: MutableMap<String, Any> = LinkedHashMap(record)
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
        val schemaName = if (content.schema == null) "TransformedRecord" else content.schema.getName()
        val outSchema = inferSchema(schemaName, transformed.first())

        return ProcessorResult.Single(ff.withContent(RecordContent(transformed, outSchema)))
    }

    private fun applyOp(dict: MutableMap<String, Any>, d: Directive) {
        when (d.op) {
            "rename" -> dict.remove(d.target)?.let { dict[d.expression] = it }
            "remove" -> dict.remove(d.target)
            "add" -> dict[d.target] = d.expression
            "copy" -> dict[d.target]?.let { dict[d.expression] = it }
            "toUpper" -> dict[d.target]?.let { v -> (v as? String)?.let { s -> dict[d.target] = s.uppercase(Locale.getDefault()) } }
            "toLower" -> dict[d.target]?.let { v -> (v as? String)?.let { s -> dict[d.target] = s.lowercase(Locale.getDefault()) } }
            "default" -> dict.computeIfAbsent(d.target) { d.expression }
            "compute" -> {
                val ctx = MapContext().apply {
                    set("record", dict)
                    dict.entries.forEach { (key, value) -> set(key, value) }
                }
                try {
                    dict[d.target] = d.compiled!!.evaluate(ctx)
                } catch (_: JexlException) {
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

        private val KNOWN_OPS: Set<String> = setOf(
                "rename", "remove", "add", "copy", "toUpper", "toLower", "default", "compute"
        )

        /** Build a flat Avro Schema from a record's keys + value types.
         * Matches the inference logic in ConvertJSONToRecord / ConvertCSVToRecord. */
        private fun inferSchema(name: String, record: Map<String, Any>): Schema? {
            if (record.isEmpty()) return null
            val defs = record.entries.joinToString { (key, value) ->
                "$key:${inferType(value)}"
            }
            return SchemaDefs.parse(name, defs)
        }

        private fun inferType(v: Any): String {
            return when(v) {
                is Boolean -> "boolean"
                is Int -> "int"
                is Long -> "long"
                is Float -> "float"
                is Double -> "double"
                is ByteArray -> "bytes"
                else -> "string"
            }
        }
    }
}
