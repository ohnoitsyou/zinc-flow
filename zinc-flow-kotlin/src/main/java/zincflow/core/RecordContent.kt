package zincflow.core

import org.apache.avro.Schema

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
@ConsistentCopyVisibility
data class RecordContent private constructor(
    val records: List<Map<String, Any>>,
    val schema: Schema? = null
) : Content {
    companion object {
        operator fun invoke(records: List<Map<String, Any>>, schema: Schema? = null): RecordContent {
            return RecordContent(records.toList(), schema)
        }
//        operator fun invoke(records: MutableList<MutableMap<String, Any>>, schema: Schema? = null): RecordContent {
//            return RecordContent(records.toList(), schema)
//        }
    }
    override fun size(): Int {
        return records.size
    }
}
