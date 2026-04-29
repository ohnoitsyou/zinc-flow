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
class ConvertAvroToRecord(schemaName: String?, fieldDefs: String?) : Processor {
    private val schemaName: String
    private val configSchema: Schema?

    init {
        this.schemaName = if (schemaName == null) "" else schemaName
        this.configSchema = SchemaDefs.parse(this.schemaName, fieldDefs)
    }

    override fun process(ff: FlowFile): ProcessorResult {
        if (ff.content !is RawContent) {
            return ProcessorResult.failure(
                "ConvertAvroToRecord: expected RawContent, got " + ff.content.javaClass.getSimpleName(), ff
            )
        }

        var schema = configSchema
        if (schema == null) {
            val attrFields = ff.attributes.get("avro.schema")
            if (attrFields != null && !attrFields.isBlank()) {
                schema = SchemaDefs.parse(schemaName, attrFields)
            }
        }
        if (schema == null || schema.getFields().isEmpty()) {
            return ProcessorResult.failure(
                "no schema: set 'fields' config or 'avro.schema' attribute", ff
            )
        }

        try {
            val reader = GenericDatumReader<GenericRecord?>(schema)
            val decoder = DecoderFactory.get().binaryDecoder(ByteArrayInputStream(raw.bytes), null)
            val records: MutableList<MutableMap<String?, Any?>?> = ArrayList<MutableMap<String?, Any?>?>()
            while (!decoder.isEnd()) {
                val r = reader.read(null, decoder)
                records.add(AvroConversion.toMap(r))
            }
            if (records.isEmpty()) {
                return ProcessorResult.failure("no records decoded from Avro binary", ff)
            }
            return ProcessorResult.single(
                ff.withContent(RecordContent(records, schema))
                    .withAttribute("record.count", records.size.toString())
            )
        } catch (eof: EOFException) {
            return ProcessorResult.failure("ConvertAvroToRecord: unexpected EOF mid-record", ff)
        } catch (ex: IOException) {
            return ProcessorResult.failure("ConvertAvroToRecord: decode failed — " + ex.message, ff)
        }
    }
}
