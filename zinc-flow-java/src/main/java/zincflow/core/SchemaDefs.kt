package zincflow.core

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.SchemaBuilder.FieldAssembler
import java.util.Locale

/** Parser for the compact field-defs config string
 * (`"name:type,name:type"`) into an Apache Avro [Schema].
 * 
 * Mirrors zinc-flow-csharp's `ConvertAvroToRecord.ParseFieldDefs`
 * (StdLib/RecordProcessors.cs:54-83). Keeps the same type aliases and
 * the same error wording so config failures are recognizable across
 * tracks. Delimiter is comma (C# canonical for this field).
 * 
 * Supported types: boolean/bool, int/int32, long/int64, float/float32,
 * double/float64, bytes, string. Unknown types throw — matches C#. */
object SchemaDefs {
    fun parse(recordName: String, fieldDefs: String): Schema? {
        if (fieldDefs.isBlank()) return null

        val name = recordName.ifBlank { "Record" }
        var assembler = SchemaBuilder.record(name).namespace("zincflow").fields()

        for (part in fieldDefs.split(",").mapNotNull { f -> f.trim().takeIf { it.isNotEmpty() }}) {
            require(part.contains(':')) { "ConvertAvroToRecord: malformed field def '$part' — expected 'name:type'" }
            val name = part.substringBefore(':').trim()
            val type = part.substringAfter(':').trim().lowercase(Locale.getDefault())

            assembler = appendField(assembler, name, type)
        }
        return assembler.endRecord()
    }

    private fun appendField(a: FieldAssembler<Schema>, name: String, type: String): FieldAssembler<Schema> {
        return when (type) {
            "boolean", "bool" -> a.name(name).type().booleanType().noDefault()
            "int", "int32" -> a.name(name).type().intType().noDefault()
            "long", "int64" -> a.name(name).type().longType().noDefault()
            "float", "float32" -> a.name(name).type().floatType().noDefault()
            "double", "float64" -> a.name(name).type().doubleType().noDefault()
            "bytes" -> a.name(name).type().bytesType().noDefault()
            "string" -> a.name(name).type().stringType().noDefault()
            else -> throw IllegalArgumentException(
                "ConvertAvroToRecord: unknown field type '$type' in '$name:$type' — valid: boolean, int, long, float, double, bytes, string"
            )
        }
    }
}
