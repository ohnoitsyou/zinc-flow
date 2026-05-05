package zincflow.processors

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RawContent
import zincflow.core.RecordContent
import zincflow.core.SchemaDefs
import java.io.ByteArrayInputStream
import java.io.EOFException
import java.io.IOException

/** Avro binary → [RecordContent]. Mirrors zinc-flow-csharp's
 * `ConvertAvroToRecord` (StdLib/RecordProcessors.cs:11-84).
 * 
 * Config:
 * schemaName — record name label for the parsed schema (optional)
 * fields     — compact Avro field defs: `"name:type,name:type"`
 * (see [SchemaDefs] for supported types)
 * 
 * When `fields` is omitted, the processor falls back to the
 * `avro.schema` FlowFile attribute (same compact format). When
 * neither produces a schema, ingestion fails with a clear message —
 * Avro binary is self-describing only when paired with a schema. */
class ConvertAvroToRecord(private val schemaName: String, fieldDefs: String) : Processor {
    private val configSchema: Schema? = SchemaDefs.parse(this.schemaName, fieldDefs)

    override fun process(ff: FlowFile): ProcessorResult {
        val content = ff.content
        if (content !is RawContent) {
            return ProcessorResult.Failure(
                "ConvertAvroToRecord: expected RawContent, got " + content.javaClass.getSimpleName(), ff
            )
        }

        var schema = configSchema
        if (schema == null) {
            val attrFields = ff.attributes["avro.schema"]
            if (!attrFields.isNullOrBlank()) {
                schema = SchemaDefs.parse(schemaName, attrFields)
            }
        }
        if (schema == null || schema.fields.isEmpty()) {
            return ProcessorResult.Failure(
                "no schema: set 'fields' config or 'avro.schema' attribute", ff
            )
        }

        try {
            val reader = GenericDatumReader<GenericRecord?>(schema)
            val decoder = DecoderFactory.get().binaryDecoder(ByteArrayInputStream(content.bytes), null)
            val records = mutableListOf<Map<String, Any>>()
            while (!decoder.isEnd()) {
                val r = reader.read(null, decoder) ?: continue
                records.add(AvroConversion.toMap(r))
            }
            if (records.isEmpty()) {
                return ProcessorResult.Failure("no records decoded from Avro binary", ff)
            }
            return ProcessorResult.Single(
                ff.withContent(RecordContent(records, schema))
                    .withAttribute("record.count", records.size.toString())
            )
        } catch (_: EOFException) {
            return ProcessorResult.Failure("ConvertAvroToRecord: unexpected EOF mid-record", ff)
        } catch (ex: IOException) {
            return ProcessorResult.Failure("ConvertAvroToRecord: decode failed — " + ex.message, ff)
        }
    }
}
