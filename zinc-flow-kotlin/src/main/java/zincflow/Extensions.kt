package zincflow

fun Any?.toLongOrDefault(default: Long): Long {
    return this?.toString()?.toLongOrNull() ?: default
}

fun Any?.toIntOrDefault(default: Int): Int {
    return this?.toString()?.toIntOrNull() ?: default
}

fun Any?.toStringOrDefault(default: String): String {
    return this?.toString() ?: default
}

inline fun <T, K, V> Iterable<T>.associateNotNull(
    transform: (T) -> Pair<K, V>?
): Map<K, V> {
    return this.mapNotNull(transform).toMap()
}

fun Any?.toTypedMap(): Map<String, Any> {
    if (this == null) return mapOf()
    require(this is Map<*, *>) { "Expected a map, got " + this.javaClass.getSimpleName() }
    return this.toTypedMap()
}

fun Map<*, *>.toTypedMap(): Map<String, Any> {
    return this.entries.associateNotNull { (k, v) -> v?.let { k.toString() to v } }
}

fun Any?.stringifyMap(): Map<String, String> {
    if (this == null) return mapOf()
    require(this is Map<*, *>) { "Expected a map, got " + this.javaClass.getSimpleName() }
    return this.entries.associateNotNull { (k, v) -> "$k" to if(v == null) "" else "$v" }
}

fun Any?.toStringList(): List<String> {
    if (this == null) return listOf()
    require(this is List<*>) { "config: expected a list, got " + this.javaClass.getSimpleName() }
    return this.map { it as String }
}