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
    @JvmStatic
    fun parse(recordName: String?, fieldDefs: String?): Schema? {
        if (fieldDefs == null || fieldDefs.isBlank()) return null

        val name = if (recordName == null || recordName.isBlank()) "Record" else recordName
        var assembler = SchemaBuilder.record(name).namespace("zincflow").fields()

        for (part in fieldDefs.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()) {
            val trimmed = part.trim { it <= ' ' }
            if (trimmed.isEmpty()) continue
            val colon = trimmed.indexOf(':')
            require(!(colon <= 0 || colon == trimmed.length - 1)) { "ConvertAvroToRecord: malformed field def '" + trimmed + "' — expected 'name:type'" }
            val fname = trimmed.substring(0, colon).trim { it <= ' ' }
            val ftype = trimmed.substring(colon + 1).trim { it <= ' ' }.lowercase(Locale.getDefault())

            assembler = appendField(assembler, fname, ftype)
        }
        return assembler.endRecord()
    }

    private fun appendField(a: FieldAssembler<Schema?>, fname: String, ftype: String): FieldAssembler<Schema?> {
        return when (ftype) {
            "boolean", "bool" -> a.name(fname).type().booleanType().noDefault()
            "int", "int32" -> a.name(fname).type().intType().noDefault()
            "long", "int64" -> a.name(fname).type().longType().noDefault()
            "float", "float32" -> a.name(fname).type().floatType().noDefault()
            "double", "float64" -> a.name(fname).type().doubleType().noDefault()
            "bytes" -> a.name(fname).type().bytesType().noDefault()
            "string" -> a.name(fname).type().stringType().noDefault()
            else -> throw IllegalArgumentException(
                ("ConvertAvroToRecord: unknown field type '" + ftype + "' in '" + fname + ":" + ftype
                        + "' — valid: boolean, int, long, float, double, bytes, string")
            )
        }
    }
}
