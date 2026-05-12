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
