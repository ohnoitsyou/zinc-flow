package zincflow.fabric

/** Kind of a processor parameter — drives UI form rendering and round-trip
 * string encoding. Enum values serialize to JSON as their `name()`
 * via Jackson's default enum serializer. */
enum class ParamKind(val jsonName: String) {
    STRING("String"),
    MULTILINE("MultiLine"),
    INTEGER("Integer"),
    NUMBER("Number"),
    BOOLEAN("Boolean"),
    ENUM("Enum"),
    EXPRESSION("Expression"),
    KEY_VALUE_LIST("KeyValueList"),
    STRING_LIST("StringList"),
    SECRET("Secret"),
    ;
}
