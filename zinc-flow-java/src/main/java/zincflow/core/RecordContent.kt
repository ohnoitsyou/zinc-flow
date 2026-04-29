package zincflow.core

import org.apache.avro.Schema
import java.util.List

/** Structured records payload — a list of flat `Map<String,Object>`
 * entries carrying an optional Avro [Schema]. The schema travels
 * alongside the records so downstream format-converting processors
 * (CSV, Avro, OCF writers) can preserve field order and types.
 * 
 * Mirrors zinc-flow-csharp's `RecordContent(Schema, List<GenericRecord>)`.
 * The schema is nullable: JSON-read records and other ad-hoc sources
 * arrive without one, and processors that only need field values
 * (QueryRecord, TransformRecord, ExtractRecordField) ignore it. Format
 * writers fall back to inferring a schema from the first record when
 * the field is absent. */
class RecordContent @JvmOverloads constructor(
    records: MutableList<MutableMap<String?, Any?>?>?,
    @JvmField val schema: Schema? = null
) : Content {
    override fun size(): Int {
        return records!!.size
    }

    val records: MutableList<MutableMap<String?, Any?>?>?

    /** Backwards-compatible constructor for call sites that don't yet
     * carry a schema (JSON reads, in-memory transforms, test setup). */
    init {
        var records = records
        requireNotNull(records) { "RecordContent records must not be null — use List.of() for empty" }
        records = List.copyOf<MutableMap<String?, Any?>?>(records)
        this.records = records
        // schema may be null — see class javadoc.
    }
}
