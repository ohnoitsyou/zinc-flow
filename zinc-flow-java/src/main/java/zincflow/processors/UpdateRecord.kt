package zincflow.processors

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.commons.jexl3.JexlBuilder
import org.apache.commons.jexl3.JexlEngine
import org.apache.commons.jexl3.JexlException
import org.apache.commons.jexl3.JexlExpression
import org.apache.commons.jexl3.MapContext
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RecordContent

/** Set or derive record fields via expressions. Record-level counterpart
 * to `EvaluateExpression` (which targets FlowFile attributes).
 * Config key `updates` is a semicolon-delimited list of
 * `fieldName = expression` pairs evaluated left-to-right against
 * a mutable working copy of each record — later expressions see earlier
 * writes.
 * 
 * Mirrors zinc-flow-csharp's `UpdateRecord`. Uses Apache
 * Commons JEXL on both track sides; syntax (arithmetic, string ops,
 * ternary, coalesce) is portable between Java and C#. */
class UpdateRecord(spec: String) : Processor {
    @JvmRecord
    private data class Update(val field: String, val compiled: JexlExpression)

    private val updates: List<Update> = spec.parseSpecToUpdates()

    private fun String.parseSpecToUpdates(): List<Update> {
        require(isNotBlank()) { "Spec can not be empty" }
        val parsed = mutableListOf<Update>()
        for (pair in split(";").mapNotNull { e -> e.trim().takeIf { it.isNotEmpty() } }) {
            val eq = pair.indexOf('=')
            require(eq > 0) { "UpdateRecord: malformed pair '$pair' — expected 'field = expression'" }
            val field = pair.substringBefore("=").trim()
            val exprStr = pair.substringAfter("=").trim()

            require(!field.isEmpty()) { "UpdateRecord: empty field name in '$pair'" }
            require(!exprStr.isEmpty()) { "UpdateRecord: empty expression for field '$field'" }

            try {
                parsed.add(Update(field, JEXL.createExpression(exprStr)))
            } catch (ex: JexlException) {
                throw IllegalArgumentException("UpdateRecord: field '$field' has invalid expression '$exprStr" + "': ${ex.message}", ex)
            }
        }
        require(parsed.isNotEmpty()) { "UpdateRecord: updates spec produced no entries" }
        return parsed
    }

    override fun process(ff: FlowFile): ProcessorResult {
        val content = ff.content
        if (content !is RecordContent) {
            return ProcessorResult.Single(ff)
        }
        if (content.records.isEmpty()) {
            return ProcessorResult.Single(ff)
        }

        val out = mutableListOf<MutableMap<String, Any>>()
        for (record in content.records) {
            val dict = record.toMutableMap()
            for (update in updates) {
                // Can't process an update with no expression
                val ctx = MapContext().apply {
                    set("record", dict)
                    dict.entries.forEach { (key, value) -> set(key, value) }
                }
                try {
                    dict[update.field] = update.compiled.evaluate(ctx)
                } catch (_: JexlException) {
                    dict.remove(update.field)
                }
            }
            out.add(dict)
        }

        val schemaName = if (content.schema == null) "UpdatedRecord" else content.schema.getName()
        val outSchema = inferSchema(schemaName, out.first())
        return ProcessorResult.Single(ff.withContent(RecordContent(out, outSchema)))
    }

    companion object {
        private val JEXL: JexlEngine = JexlBuilder()
            .strict(false).safe(true).silent(false).create()

        private fun inferSchema(name: String, record: MutableMap<String, Any>): Schema {
            var schemaBuilder = SchemaBuilder.record(name).fields()
            for ((key, value) in record) {
                schemaBuilder = when (value) {
                    is Boolean -> schemaBuilder.optionalBoolean(key)
                    is Int -> schemaBuilder.optionalInt(key)
                    is Long -> schemaBuilder.optionalLong(key)
                    is Float -> schemaBuilder.optionalFloat(key)
                    is Double -> schemaBuilder.optionalDouble(key)
                    else -> schemaBuilder.optionalString(key)
                }
            }
            return schemaBuilder.endRecord()
        }
    }
}
