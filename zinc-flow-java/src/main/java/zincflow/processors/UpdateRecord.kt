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
import java.util.List

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
class UpdateRecord(spec: String?) : Processor {
    @JvmRecord
    private data class Update(val field: String?, val compiled: JexlExpression?)

    private val updates: MutableList<Update>

    init {
        val parsed: MutableList<Update?> = ArrayList<Update?>()
        if (spec != null) {
            for (pair in spec.split(";".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()) {
                val trimmed = pair.trim { it <= ' ' }
                if (trimmed.isEmpty()) continue
                val eq = trimmed.indexOf('=')
                require(eq > 0) { "UpdateRecord: malformed pair '" + trimmed + "' — expected 'field = expression'" }
                val field = trimmed.substring(0, eq).trim { it <= ' ' }
                val exprStr = trimmed.substring(eq + 1).trim { it <= ' ' }
                require(!field.isEmpty()) { "UpdateRecord: empty field name in '" + trimmed + "'" }
                require(!exprStr.isEmpty()) { "UpdateRecord: empty expression for field '" + field + "'" }
                try {
                    parsed.add(Update(field, JEXL.createExpression(exprStr)))
                } catch (ex: JexlException) {
                    throw IllegalArgumentException(
                        ("UpdateRecord: field '" + field + "' has invalid expression '" + exprStr + "': "
                                + ex.message), ex
                    )
                }
            }
        }
        require(!parsed.isEmpty()) { "UpdateRecord: updates spec produced no entries" }
        this.updates = List.copyOf<Update?>(parsed)
    }

    override fun process(ff: FlowFile): ProcessorResult {
        if (ff.content !is RecordContent) {
            return ProcessorResult.single(ff)
        }
        if (rc.records.isEmpty()) {
            return ProcessorResult.single(ff)
        }

        val out: MutableList<MutableMap<String?, Any?>?> = ArrayList<MutableMap<String?, Any?>?>(rc.records.size)
        for (record in rc.records) {
            val dict: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>(record)
            for (u in updates) {
                val ctx = MapContext()
                ctx.set("record", dict)
                for (e in dict.entries) ctx.set(e.key, e.value)
                var `val`: Any?
                try {
                    `val` = u.compiled!!.evaluate(ctx)
                } catch (ex: JexlException) {
                    `val` = null
                }
                dict.put(u.field, `val`)
            }
            out.add(dict)
        }

        val schemaName: String? = if (rc.schema == null) "UpdatedRecord" else rc.schema.getName()
        val outSchema: Schema? = inferSchema(schemaName, out.getFirst())
        return ProcessorResult.single(ff.withContent(RecordContent(out, outSchema)))
    }

    companion object {
        private val JEXL: JexlEngine = JexlBuilder()
            .strict(false).safe(true).silent(false).create()

        private fun inferSchema(name: String?, record: MutableMap<String?, Any?>): Schema? {
            if (record.isEmpty()) return null
            var fa = SchemaBuilder.record(name).fields()
            for (e in record.entries) {
                val v = e.value
                if (v == null) fa = fa.nullableString(e.key, "")
                else if (v is Boolean) fa = fa.optionalBoolean(e.key)
                else if (v is Int) fa = fa.optionalInt(e.key)
                else if (v is Long) fa = fa.optionalLong(e.key)
                else if (v is Float) fa = fa.optionalFloat(e.key)
                else if (v is Double) fa = fa.optionalDouble(e.key)
                else fa = fa.optionalString(e.key)
            }
            return fa.endRecord()
        }
    }
}
