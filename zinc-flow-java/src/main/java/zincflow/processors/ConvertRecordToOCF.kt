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
class ConvertRecordToOCF @JvmOverloads constructor(codecName: String? = "null") : Processor {
    private val codec: CodecFactory?

    init {
        this.codec = CodecFactory.fromString(if (codecName == null || codecName.isEmpty()) "null" else codecName)
    }

    override fun process(ff: FlowFile): ProcessorResult {
        if (ff.content !is RecordContent) {
            return ProcessorResult.failure(
                "ConvertRecordToOCF: expected RecordContent, got " + ff.content.javaClass.getSimpleName(), ff
            )
        }
        if (rc.records.isEmpty()) {
            return ProcessorResult.single(ff)
        }
        val schema: Schema? = rc.schema
        if (schema == null) {
            return ProcessorResult.failure(
                "ConvertRecordToOCF: RecordContent has no schema — upstream must declare one", ff
            )
        }

        val datum = GenericDatumWriter<GenericRecord?>(schema)
        val buf = ByteArrayOutputStream()
        try {
            DataFileWriter<GenericRecord?>(datum).use { writer ->
                writer.setCodec(codec)
                writer.create(schema, buf)
                for (record in rc.records) {
                    writer.append(AvroConversion.toGenericRecord(record, schema))
                }
                writer.flush()
                return ProcessorResult.single(ff.withContent(RawContent(buf.toByteArray())))
            }
        } catch (ex: IOException) {
            return ProcessorResult.failure("ConvertRecordToOCF: write failed — " + ex.message, ff)
        }
    }
}
