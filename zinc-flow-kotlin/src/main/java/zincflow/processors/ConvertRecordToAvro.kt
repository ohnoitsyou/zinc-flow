package zincflow.processors

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RawContent
import zincflow.core.RecordContent
import zincflow.core.SchemaDefs
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.StringJoiner

/** [RecordContent] → Avro binary bytes. Mirrors zinc-flow-csharp's
 * `ConvertRecordToAvro` (StdLib/RecordProcessors.cs:90-105) — no
 * config, schema is read directly from the incoming RecordContent.
 * 
 * Writes an `avro.schema` attribute on the output FlowFile with the
 * compact field-defs representation so a downstream
 * [ConvertAvroToRecord] can decode without repeating the schema in
 * its config. */
class ConvertRecordToAvro : Processor {
    override fun process(ff: FlowFile): ProcessorResult {
        val content = ff.content
        if (ff.content !is RecordContent) {
            return ProcessorResult.Failure(
                "ConvertRecordToAvro: expected RecordContent, got " + ff.content.javaClass.getSimpleName(), ff
            )
        }
        if (content.records.isEmpty()) {
            return ProcessorResult.Single(ff)
        }
        val schema: Schema = content.schema ?: return ProcessorResult.Failure(
            "ConvertRecordToAvro: RecordContent has no schema — upstream must declare one", ff
        )

        val writer = GenericDatumWriter<GenericRecord?>(schema)
        val buf = ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(buf, null)
        try {
            for (record in content.records) {
                writer.write(AvroConversion.toGenericRecord(record, schema), encoder)
            }
            encoder.flush()
            return ProcessorResult.Single(
                ff.withContent(RawContent(buf.toByteArray()))
                    .withAttribute("avro.schema", compactFieldDefs(schema))
            )
        } catch (ex: IOException) {
            return ProcessorResult.Failure("ConvertRecordToAvro: encode failed — " + ex.message, ff)
        }
    }

    companion object {
        /** Serialize a Schema as the compact `"name:type,name:type"`
         * form so downstream processors can re-parse it with
         * [SchemaDefs.parse]. */
        private fun compactFieldDefs(schema: Schema): String {
            val sj = StringJoiner(",")
            for (f in schema.fields) {
                val t = if (f.schema().type == Schema.Type.UNION)
                    firstNonNullBranch(f.schema())
                else
                    f.schema().type
                sj.add(f.name() + ":" + t.getName())
            }
            return sj.toString()
        }

        private fun firstNonNullBranch(union: Schema): Schema.Type {
            for (branch in union.types) {
                if (branch.type != Schema.Type.NULL) return branch.type
            }
            return Schema.Type.NULL
        }
    }
}
