package zincflow.processors

import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RawContent
import zincflow.core.RecordContent
import java.io.ByteArrayOutputStream
import java.io.IOException

/** [RecordContent] → OCF bytes (Object Container File). Mirrors
 * zinc-flow-csharp's `ConvertRecordToOCF` (StdLib/RecordProcessors.cs:259-283):
 * no schema config — the schema already on the RecordContent is what
 * gets embedded in the OCF header.
 * 
 * Config:
 * codec — compression codec; `null` (default, no compression),
 * `deflate`, `snappy`, `bzip2`, `xz`,
 * `zstandard`. Passed through to
 * [CodecFactory.fromString]. */
class ConvertRecordToOCF(codecName: String?) : Processor {
    private val codec = CodecFactory.fromString(if (codecName.isNullOrEmpty()) "null" else codecName)

    override fun process(ff: FlowFile): ProcessorResult {
        val content = ff.content
        if (content !is RecordContent) {
            return ProcessorResult.Failure(
                "ConvertRecordToOCF: expected RecordContent, got " + content.javaClass.getSimpleName(), ff
            )
        }
        if (content.records.isEmpty()) {
            return ProcessorResult.Single(ff)
        }
        val schema: Schema = content.schema ?: return ProcessorResult.Failure(
            "ConvertRecordToOCF: RecordContent has no schema — upstream must declare one", ff
        )

        val datum = GenericDatumWriter<GenericRecord?>(schema)
        val buf = ByteArrayOutputStream()
        try {
            DataFileWriter<GenericRecord?>(datum).use { writer ->
                writer.setCodec(codec)
                writer.create(schema, buf)
                for (record in content.records) {
                    writer.append(AvroConversion.toGenericRecord(record, schema))
                }
                writer.flush()
                return ProcessorResult.Single(ff.withContent(RawContent(buf.toByteArray())))
            }
        } catch (ex: IOException) {
            return ProcessorResult.Failure("ConvertRecordToOCF: write failed — $ex", ff)
        }
    }
}
