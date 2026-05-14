package zincflow.fabric

/** Shared type-ref + version-comparison helpers used by every registry
 * (processor, source, provider). Keeps the parse + compare rules in
 * one place so `Foo@1.2.3` means exactly the same thing no
 * matter which kind of plugin is being looked up. */
object TypeRefs {
    const val DEFAULT_VERSION: String = "1.0.0"

    /** Compare two dotted version strings. A trailing `".0"`
     * difference (`"1.2"` vs `"1.2.0"`) sorts as equal;
     * non-numeric segments compare lexicographically. */
    fun compareVersions(a: String, b: String): Int {
        val aSplit: List<String> = a.split(".")
        val bSplit: List<String> = b.split(".")
        val len = maxOf(aSplit.size, bSplit.size)
        for (i in 0..<len) {
            val ap = if (i < aSplit.size) aSplit[i] else "0"
            val bp = if (i < bSplit.size) bSplit[i] else "0"
            try {
                val cmp = Integer.parseInt(ap).compareTo(Integer.parseInt(bp))
                if (cmp != 0) return cmp
            } catch (_: NumberFormatException) {
                val cmp = ap.compareTo(bp)
                if (cmp != 0) return cmp
            }
        }
        return 0
    }

    fun qualify(name: String, version: String): String {
        return "$name@$version"
    }

    /** Parsed form of a config `type:` string — either bare
     * (`"Foo"`, meaning "latest") or pinned (`"Foo@1.2.3"`). */
    @JvmRecord
    data class TypeRef(val name: String, val version: String?) {
        fun raw(): String {
            return if (version == null) name else "$name@$version"
        }

        companion object {
            fun parse(raw: String?): TypeRef {
                if (raw == null) return TypeRef("", null)
                val at: Int = raw.indexOf('@')
                if (at < 0) return TypeRef(raw, null)
                return TypeRef(raw.substring(0, at), raw.substring(at + 1))
            }
        }
    }
}
