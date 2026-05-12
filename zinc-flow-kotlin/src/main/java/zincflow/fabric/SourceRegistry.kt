package zincflow.fabric

import zincflow.core.Source
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.listOf

/** Registry of source factories keyed by `name@version`. Mirror
 * of [ProcessorRegistry] for [Source] plugins. Config resolves
 * `type: GetFile@1.0.0` → factory → instance the same way
 * processors do; a bare `type: GetFile` falls through to the
 * latest registered version. */
class SourceRegistry {
    fun interface Factory {
        fun create(name: String, config: Map<String, Any>): Source?
    }

    class TypeInfo(
        val name: String,
        val version: String,
        val description: String = "",
        val configKeys: List<String> = listOf()
    ) {
        fun qualifiedName(): String {
            return TypeRefs.qualify(name, version)
        }
    }

    private val versioned = ConcurrentHashMap<String, Factory>()
    private val latestVersion = ConcurrentHashMap<String, String>()
    private val metadata = ConcurrentHashMap<String, TypeInfo>()

    fun register(info: TypeInfo, factory: Factory) {
        val key = info.qualifiedName()
        versioned[key] = factory
        metadata[key] = info
        latestVersion.merge(info.name, info.version) { oldV: String, newV: String ->
            if (TypeRefs.compareVersions(oldV, newV ) >= 0) oldV else newV
        }
    }

    fun register(type: String, factory: Factory) {
        register(TypeInfo(type, TypeRefs.DEFAULT_VERSION, "", mutableListOf()), factory)
    }

    fun has(type: String): Boolean {
        return resolveKey(type) != null
    }

    private fun resolveKey(type: String): String? {
        if (type.isEmpty()) return null
        if (type.contains("@")) return (if (versioned.containsKey(type)) type else null)
        val latest = latestVersion[type]
        return (if (latest == null) null else TypeRefs.qualify(type, latest))
    }

    fun create(type: String, name: String, config: MutableMap<String, Any>): Source? {
        val key = resolveKey(type) ?: throw IllegalArgumentException("$type is not defined")
        return versioned[key]?.create(name, config)
    }

    fun listAll(): MutableList<TypeInfo> {
        return metadata.values
            .sortedWith(Comparator.comparing(TypeInfo::name)
                .thenComparing(TypeInfo::version) { a, b ->
                    TypeRefs.compareVersions(a, b)
                }
            ).toMutableList()
    }

    fun listVersions(type: String): MutableList<TypeInfo> {
        return metadata.values.filter { it.name == type }.sortedWith(
            Comparator.comparing(TypeInfo::version) { a, b -> TypeRefs.compareVersions(a, b) }
        ).toMutableList()
    }

    fun latest(type: String): TypeInfo? {
        val latest = latestVersion[type] ?: return null
        return metadata[TypeRefs.qualify(type, latest)]
    }
}
