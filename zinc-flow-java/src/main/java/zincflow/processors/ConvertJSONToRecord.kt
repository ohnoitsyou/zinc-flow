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
import java.util.List
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
        if (ff.content !is RawContent) {
            return ProcessorResult.failure(
                "ConvertJSONToRecord: expected RawContent, got " + ff.content.javaClass.getSimpleName(), ff
            )
        }
        val text = String(raw.bytes, StandardCharsets.UTF_8).trim { it <= ' ' }
        if (text.isEmpty()) {
            return ProcessorResult.failure("ConvertJSONToRecord: empty payload", ff)
        }
        try {
            val records: MutableList<MutableMap<String?, Any?>>
            if (text.startsWith("[")) {
                records = MAPPER.readValue<MutableList<MutableMap<String?, Any?>>>(text, ARR_TYPE)
            } else {
                records =
                    List.of<MutableMap<String?, Any?>?>(MAPPER.readValue<MutableMap<String?, Any?>?>(text, OBJ_TYPE))
            }
            val schema = inferSchema(records)
            return ProcessorResult.single(
                ff.withContent(RecordContent(records, schema))
                    .withAttribute("record.count", records.size.toString())
            )
        } catch (ex: Exception) {
            return ProcessorResult.failure("ConvertJSONToRecord: parse failed — " + ex.message, ff)
        }
    }

    /** Infer a flat Avro schema from the first record's keys + value
     * types. String / int / long / double / boolean / bytes are
     * detected directly; everything else (nested objects, arrays) falls
     * back to string — not strictly correct but keeps JSON→Record
     * usable as input to Avro writers for the common flat-object case. */
    private fun inferSchema(records: MutableList<MutableMap<String?, Any?>>): Schema? {
        if (records.isEmpty()) return null
        val first: MutableMap<String?, Any?> = records.getFirst()
        val defs = StringJoiner(",")
        for (entry in first.entries) {
            defs.add(entry.key + ":" + inferType(entry.value))
        }
        return SchemaDefs.parse(schemaName, defs.toString())
    }

    companion object {
        private val MAPPER = ObjectMapper()
        private val OBJ_TYPE: TypeReference<MutableMap<String?, Any?>?> =
            object : TypeReference<MutableMap<String?, Any?>?>() {}
        private val ARR_TYPE: TypeReference<MutableList<MutableMap<String?, Any?>?>?> =
            object : TypeReference<MutableList<MutableMap<String?, Any?>?>?>() {}

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
