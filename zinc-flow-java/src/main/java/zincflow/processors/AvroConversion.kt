package zincflow.processors

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import zincflow.core.RecordContent
import java.nio.ByteBuffer
import java.util.List

/** Bidirectional helpers between Avro's [GenericRecord] and the
 * `Map<String,Object>` shape that [RecordContent]
 * uses.  Avro's native types don't map 1-to-1 to Java collections
 * (strings come back as [Utf8], byte fields as [ByteBuffer],
 * unions need branch selection), so this centralises the translation.
 * 
 * Not a full-featured codec — we cover the primitive + nested record +
 * array + map + nullable-union shapes that real pipelines use. Fixed,
 * enum, decimal, and other logical types can land in follow-up work. */
internal object AvroConversion {
    // --- Record ↔ Map -----------------------------------------------------
    fun toMap(record: GenericRecord): Map<String, Any?> {
        val schema = record.schema
        return schema.fields.associate { field -> field.name() to unwrap(record.get(field.name()), field.schema()) }
    }

    fun toGenericRecord(map: MutableMap<String?, Any?>?, schema: Schema): GenericRecord? {
        if (map == null) return null
        val record = GenericData.Record(schema)
        for (f in schema.getFields()) {
            val raw = map.get(f.name())
            record.put(f.name(), wrap(raw, f.schema()))
        }
        return record
    }

    // --- Value unwrap (Avro → Java idiomatic) ----------------------------
    private fun unwrap(value: Any?, schema: Schema): Any? {
        if (value == null) return null
        val effective = resolveUnion(schema, value)
        return when (effective.getType()) {
            Schema.Type.STRING -> if (value is Utf8) value.toString() else value.toString()
            Schema.Type.BYTES -> if (value is ByteBuffer) toByteArray(value) else value
            Schema.Type.RECORD -> toMap(value as GenericRecord)
            Schema.Type.ARRAY -> {
                val src = value as MutableList<Any?>
                val out: MutableList<Any?> = ArrayList<Any?>(src.size)
                for (item in src) out.add(unwrap(item, effective.getElementType()))
                out
            }

            Schema.Type.MAP -> {
                val src = value as MutableMap<CharSequence?, Any?>
                val out: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
                for (entry in src.entries) {
                    out.put(entry.key.toString(), unwrap(entry.value, effective.getValueType()))
                }
                out
            }

            else -> value
        }
    }

    // --- Value wrap (Java idiomatic → Avro) ------------------------------
    private fun wrap(value: Any?, schema: Schema): Any? {
        if (value == null) return null
        val effective = resolveUnion(schema, value)
        return when (effective.getType()) {
            Schema.Type.STRING -> if (value is CharSequence) value.toString() else value.toString()
            Schema.Type.BYTES -> if (value is ByteArray) ByteBuffer.wrap(value) else value
            Schema.Type.RECORD -> if (value is MutableMap<*, *>)
                toGenericRecord(value as MutableMap<String?, Any?>, effective)
            else
                value

            Schema.Type.ARRAY -> {
                val src = if (value is MutableList<*>) value as MutableList<Any?> else List.of<Any?>(value)
                val out: MutableList<Any?> = ArrayList<Any?>(src.size)
                for (item in src) out.add(wrap(item, effective.getElementType()))
                out
            }

            Schema.Type.MAP -> {
                val src = value as MutableMap<String?, Any?>
                val out: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
                for (entry in src.entries) {
                    out.put(entry.key, wrap(entry.value, effective.getValueType()))
                }
                out
            }

            Schema.Type.INT -> if (value is Number) value.toInt() else value
            Schema.Type.LONG -> if (value is Number) value.toLong() else value
            Schema.Type.FLOAT -> if (value is Number) value.toFloat() else value
            Schema.Type.DOUBLE -> if (value is Number) value.toDouble() else value
            else -> value
        }
    }

    // --- Union handling ---------------------------------------------------
    /** For unions like `["null", "string"]`, pick the branch that
     * matches the actual value type. Falls back to the first non-null
     * branch when value is null and the union includes null. */
    private fun resolveUnion(schema: Schema, value: Any?): Schema {
        if (schema.getType() != Schema.Type.UNION) return schema
        for (branch in schema.getTypes()) {
            if (branchMatches(branch, value)) return branch
        }
        // Fall back to the first non-null branch — common for nullable
        // fields where we're writing a non-null value.
        for (branch in schema.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) return branch
        }
        return schema.getTypes().getFirst()
    }

    private fun branchMatches(branch: Schema, value: Any?): Boolean {
        return when (branch.getType()) {
            Schema.Type.NULL -> value == null
            Schema.Type.STRING -> value is CharSequence
            Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE -> value is Number
            Schema.Type.BOOLEAN -> value is Boolean
            Schema.Type.BYTES -> value is ByteArray || value is ByteBuffer
            Schema.Type.ARRAY -> value is MutableList<*>
            Schema.Type.MAP -> value is MutableMap<*, *>
            Schema.Type.RECORD -> value is GenericRecord || value is MutableMap<*, *>
            else -> false
        }
    }

    private fun toByteArray(bb: ByteBuffer): ByteArray {
        var bb = bb
        bb = bb.duplicate()
        val out = ByteArray(bb.remaining())
        bb.get(out)
        return out
    }
}
