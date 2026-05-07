package zincflow.fabric

/** Kind of a processor parameter — drives UI form rendering and round-trip
 * string encoding. Enum values serialize to JSON as their `name()`
 * via Jackson's default enum serializer. */
enum class ParamKind {
    STRING,
    MULTILINE,
    INTEGER,
    NUMBER,
    BOOLEAN,
    ENUM,
    EXPRESSION,
    KEY_VALUE_LIST,
    STRING_LIST,
    SECRET;

    /** PascalCase form matching the C# track's JSON output, so the React
     * UI sees identical kind strings regardless of worker language. */
    fun jsonName(): String {
        return when (this) {
            ParamKind.STRING -> "String"
            ParamKind.MULTILINE -> "Multiline"
            ParamKind.INTEGER -> "Integer"
            ParamKind.NUMBER -> "Number"
            ParamKind.BOOLEAN -> "Boolean"
            ParamKind.ENUM -> "Enum"
            ParamKind.EXPRESSION -> "Expression"
            ParamKind.KEY_VALUE_LIST -> "KeyValueList"
            ParamKind.STRING_LIST -> "StringList"
            ParamKind.SECRET -> "Secret"
        }
    }
}
