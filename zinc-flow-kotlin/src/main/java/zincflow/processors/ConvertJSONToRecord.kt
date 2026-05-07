package zincflow.processors

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.Schema
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RawContent
import zincflow.core.RecordContent
import zincflow.core.SchemaDefs
import java.nio.charset.StandardCharsets
import java.util.StringJoiner

/** Parses the FlowFile's RawContent payload as JSON and upgrades it to
 * [RecordContent]. Mirrors zinc-flow-csharp's
 * `ConvertJSONToRecord` (StdLib/Processors.cs:148-187).
 * 
 * Config:
 * schemaName — record name label applied to the inferred schema
 * 
 * Accepts either a single JSON object (wrapped into a 1-element record
 * list) or a JSON array of objects. The schema is inferred from the
 * first record's keys + value types so downstream Avro/OCF writers can
 * encode without extra config. */
class ConvertJSONToRecord @JvmOverloads constructor(schemaName: String? = "") : Processor {
    private val schemaName: String

    init {
        this.schemaName = if (schemaName == null || schemaName.isBlank()) "JsonRecord" else schemaName
    }

    override fun process(ff: FlowFile): ProcessorResult {
        val content = ff.content
        if (content !is RawContent) {
            return ProcessorResult.Failure(
                "ConvertJSONToRecord: expected RawContent, got " + content.javaClass.getSimpleName(), ff
            )
        }
        val text = String(content.bytes, StandardCharsets.UTF_8).trim { it <= ' ' }
        if (text.isEmpty()) {
            return ProcessorResult.Failure("ConvertJSONToRecord: empty payload", ff)
        }
        try {
            val records = if (text.startsWith("[")) {
                MAPPER.readValue(text, ARR_TYPE)
            } else {
                listOf<MutableMap<String, Any>>(MAPPER.readValue(text, OBJ_TYPE))
            }
            val schema = inferSchema(records)
            return ProcessorResult.Single(
                ff.withContent(RecordContent(records, schema))
                    .withAttribute("record.count", records.size.toString())
            )
        } catch (ex: Exception) {
            return ProcessorResult.Failure("ConvertJSONToRecord: parse failed — " + ex.message, ff)
        }
    }

    /** Infer a flat Avro schema from the first record's keys + value
     * types. String / int / long / double / boolean / bytes are
     * detected directly; everything else (nested objects, arrays) falls
     * back to string — not strictly correct but keeps JSON→Record
     * usable as input to Avro writers for the common flat-object case. */
    private fun inferSchema(records: List<MutableMap<String, Any>>): Schema? {
        if (records.isEmpty()) return null
        val definition = records.first().entries.joinToString { (key, value) ->
            "$key:${inferType(value)}"
        }
        return SchemaDefs.parse(schemaName, definition)
    }

    companion object {
        private val MAPPER = ObjectMapper()
        private val OBJ_TYPE: TypeReference<MutableMap<String, Any>> =
            object : TypeReference<MutableMap<String, Any>>() {}
        private val ARR_TYPE: TypeReference<MutableList<MutableMap<String, Any>>> =
            object : TypeReference<MutableList<MutableMap<String, Any>>>() {}

        private fun inferType(v: Any?): String {
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
