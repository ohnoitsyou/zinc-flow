package zincflow.processors

import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RawContent
import zincflow.core.RecordContent
import java.io.IOException

/** Object Container File (OCF, the on-disk Avro format) → RecordContent.
 * The schema is embedded in the file header, so no config is required.
 * Surfaces the schema JSON as the `avro.schema` attribute so a
 * downstream ConvertRecordToOCF / ConvertRecordToAvro can round-trip
 * without duplicating schema config. */
class ConvertOCFToRecord : Processor {
    override fun process(ff: FlowFile): ProcessorResult {
        if (ff.content !is RawContent) {
            return ProcessorResult.failure(
                "ConvertOCFToRecord: expected RawContent, got " + ff.content.javaClass.getSimpleName(), ff
            )
        }
        val reader = GenericDatumReader<GenericRecord?>()
        try {
            DataFileReader<GenericRecord?>(SeekableByteArrayInput(raw.bytes), reader).use { file ->
                val records: MutableList<MutableMap<String?, Any?>?> = ArrayList<MutableMap<String?, Any?>?>()
                var record: GenericRecord? = null
                while (file.hasNext()) {
                    record = file.next(record) // reuse instance for perf
                    records.add(AvroConversion.toMap(record))
                }
                val schemaJson: String? = file.getSchema().toString()
                return ProcessorResult.single(
                    ff.withContent(RecordContent(records))
                        .withAttribute("record.count", records.size.toString())
                        .withAttribute("avro.schema", schemaJson)
                )
            }
        } catch (ex: IOException) {
            return ProcessorResult.failure("ConvertOCFToRecord: read failed — " + ex.message, ff)
        }
    }
}
