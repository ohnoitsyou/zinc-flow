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
        if (ff.content !is RecordContent) {
            return ProcessorResult.failure(
                "ConvertRecordToAvro: expected RecordContent, got " + ff.content.javaClass.getSimpleName(), ff
            )
        }
        if (rc.records.isEmpty()) {
            return ProcessorResult.single(ff)
        }
        val schema: Schema? = rc.schema
        if (schema == null) {
            return ProcessorResult.failure(
                "ConvertRecordToAvro: RecordContent has no schema — upstream must declare one", ff
            )
        }

        val writer = GenericDatumWriter<GenericRecord?>(schema)
        val buf = ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(buf, null)
        try {
            for (record in rc.records) {
                writer.write(AvroConversion.toGenericRecord(record, schema), encoder)
            }
            encoder.flush()
            return ProcessorResult.single(
                ff.withContent(RawContent(buf.toByteArray()))
                    .withAttribute("avro.schema", compactFieldDefs(schema))
            )
        } catch (ex: IOException) {
            return ProcessorResult.failure("ConvertRecordToAvro: encode failed — " + ex.message, ff)
        }
    }

    companion object {
        /** Serialize a Schema as the compact `"name:type,name:type"`
         * form so downstream processors can re-parse it with
         * [SchemaDefs.parse]. */
        private fun compactFieldDefs(schema: Schema): String? {
            val sj = StringJoiner(",")
            for (f in schema.getFields()) {
                val t = if (f.schema().getType() == Schema.Type.UNION)
                    firstNonNullBranch(f.schema())
                else
                    f.schema().getType()
                sj.add(f.name() + ":" + t.getName())
            }
            return sj.toString()
        }

        private fun firstNonNullBranch(union: Schema): Schema.Type {
            for (branch in union.getTypes()) {
                if (branch.getType() != Schema.Type.NULL) return branch.getType()
            }
            return Schema.Type.NULL
        }
    }
}
