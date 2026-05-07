package zincflow.processors

import com.fasterxml.jackson.databind.MappingIterator
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import org.apache.avro.Schema
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RawContent
import zincflow.core.RecordContent
import zincflow.core.SchemaDefs
import java.io.IOException
import java.nio.charset.StandardCharsets

/** Parses CSV text into [RecordContent]. Mirrors zinc-flow-csharp's
 * `ConvertCSVToRecord` (StdLib/RecordProcessors.cs:301-340).
 * 
 * Config:
 * schemaName — record name label (optional)
 * delimiter  — single char (default ',')
 * hasHeader  — first row is a header (default true)
 * fields     — compact Avro field defs (`"name:type,name:type"`);
 * when set, drives the output schema and column order.
 * When absent, a string-typed schema is inferred from the
 * header row or the `hasHeader=false` raw shape. */
class ConvertCSVToRecord @JvmOverloads constructor(
    private val schemaName: String = "",
    private val delimiter: Char = ',',
    private val hasHeader: Boolean = true,
    fieldDefs: String = ""
) : Processor {
    private val explicitSchema: Schema? = SchemaDefs.parse(this.schemaName, fieldDefs)

    override fun process(ff: FlowFile): ProcessorResult {
        val content = ff.content
        if (content !is RawContent) {
            return ProcessorResult.Failure(
                "ConvertCSVToRecord: expected RawContent, got " + content.javaClass.getSimpleName(), ff
            )
        }
        var sb = CsvSchema.builder().setColumnSeparator(delimiter)
        if (explicitSchema != null) {
            for (f in explicitSchema.getFields()) sb.addColumn(f.name())
            sb.setUseHeader(hasHeader) // header row consumed but schema-field order wins
        } else if (hasHeader) {
            sb = sb.setUseHeader(true)
        }
        val csvSchema = sb.build()

        try {
            val text = String(content.bytes, StandardCharsets.UTF_8)
            val it: MappingIterator<MutableMap<String, Any>> = MAPPER
                .readerFor(MutableMap::class.java)
                .with(csvSchema)
                .readValues(text)
            val records = mutableListOf<MutableMap<String, Any>>()
            while (it.hasNext()) records.add(it.next())
            val effective = explicitSchema ?: inferStringSchema(records)
            return ProcessorResult.Single(
                ff.withContent(RecordContent(records, effective))
                    .withAttribute("record.count", records.size.toString())
            )
        } catch (ex: IOException) {
            return ProcessorResult.Failure("ConvertCSVToRecord: parse failed — " + ex.message, ff)
        }
    }

    /** Build a string-typed schema from the first record's keys when no
     * explicit field defs were provided. CSV values land as strings
     * (no type inference without a schema hint) so this is a faithful
     * representation of what's in the RecordContent. */
    private fun inferStringSchema(records: MutableList<MutableMap<String, Any>>): Schema? {
        if (records.isEmpty()) return null
        val defs = StringBuilder()
        var first = true
        for (key in records.first().keys) {
            if (!first) defs.append(',')
            defs.append(key).append(":string")
            first = false
        }
        val name = schemaName.ifEmpty { "CsvRecord" }
        return SchemaDefs.parse(name, defs.toString())
    }

    companion object {
        private val MAPPER = CsvMapper()
    }
}
