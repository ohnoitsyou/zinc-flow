package zincflow.processors

import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RawContent
import zincflow.core.RecordContent

/** Serializes [RecordContent] to CSV text. Mirrors zinc-flow-csharp's
 * `ConvertRecordToCSV` (StdLib/RecordProcessors.cs:346-364).
 * 
 * Config:
 * delimiter     — single char (default ',')
 * includeHeader — write the column header row (default true)
 * 
 * Column order is driven by the first record's key insertion order
 * (LinkedHashMap preserves this from JSON/Avro reads). No explicit
 * column-override config — matches C#, which relies on the
 * RecordContent schema. */
class ConvertRecordToCSV @JvmOverloads constructor(
    private val delimiter: Char = ',',
    private val includeHeader: Boolean = true
) : Processor {
    override fun process(ff: FlowFile): ProcessorResult {
        val content = ff.content
        if (content !is RecordContent) {
            return ProcessorResult.Failure(
                "ConvertRecordToCSV: expected RecordContent, got " + content.javaClass.getSimpleName(), ff
            )
        }
        val records: List<Map<String, Any>> = content.records

        if (records.isEmpty()) {
            // Empty record list → empty payload. Can't emit a header
            // without a first record to source keys from.
            return ProcessorResult.Single(ff.withContent(RawContent(ByteArray(0))))
        }

        val columns = records.first().keys.toList()
        val sb = CsvSchema.builder().setColumnSeparator(delimiter)
        for (c in columns) sb.addColumn(c)
        val schema = sb.build().withUseHeader(includeHeader)

        try {
            val bytes: ByteArray = MAPPER.writer(schema).writeValueAsBytes(records)
            return ProcessorResult.Single(ff.withContent(RawContent(bytes)))
        } catch (ex: Exception) {
            return ProcessorResult.Failure("ConvertRecordToCSV: serialize failed — $ex", ff)
        }
    }

    companion object {
        private val MAPPER = CsvMapper()
    }
}
