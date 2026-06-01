package zincflow.fabric

import zincflow.core.Provider
import java.util.concurrent.ConcurrentHashMap

/** Registry of provider factories keyed by `name@version`.
 * Parallel to [ProcessorRegistry] / [SourceRegistry]. Callers
 * resolve a config `type: LoggingProvider@1.0.0` to a factory
 * at load time; a bare `type: LoggingProvider` picks the
 * latest version.
 * 
 * Providers are conceptually singletons per type within one worker,
 * but the registry supports multiple versions to make plugin-based
 * replacement work: a plugin jar shipping a newer version of the
 * same type wins the latest-version lookup without changing any
 * framework code. */
class ProviderRegistry {

    private val versioned = ConcurrentHashMap<String, Factory>()
    private val latestVersion = ConcurrentHashMap<String, String>()
    private val metadata = ConcurrentHashMap<String, TypeInfo>()

    fun interface Factory {
        fun create(config: Map<String, Any>): Provider?
    }

    class TypeInfo(
        val name: String,
        val version: String,
        val description: String,
        val configKeys: List<String>
    ) {
        val qualifiedName = TypeRefs.qualify(name, version)
    }

    fun register(info: TypeInfo, factory: Factory) {
        val key = info.qualifiedName
        versioned[key] = factory
        metadata[key] = info
        latestVersion.merge(info.name, info.version) { oldV: String, newV: String -> if (TypeRefs.compareVersions(oldV, newV) >= 0) oldV else newV }
    }

    fun register(type: String, factory: Factory) {
        register(TypeInfo(type, TypeRefs.DEFAULT_VERSION, "", listOf()), factory)
    }

    fun has(type: String): Boolean {
        return resolveKey(type) != null
    }

    private fun resolveKey(type: String): String? {
        if (type.isEmpty()) return null
        if (type.contains("@")) return (if (versioned.containsKey(type)) type else null)
        val latest = latestVersion[type]
        return if (latest == null) null else TypeRefs.qualify(type, latest)
    }

    fun create(type: String, config: Map<String, Any>): Provider? {
        val key = resolveKey(type)
        requireNotNull(key) { "ProviderRegistry: unknown provider type '$type'" }
        return versioned[key]?.create(config)
    }

    fun listAll(): MutableList<TypeInfo> {
        return metadata.values.toList()
            .sortedWith(Comparator.comparing(TypeInfo::name)
                .thenComparing(TypeInfo::version) { a: String, b: String ->
                    TypeRefs.compareVersions(a, b)
                }
            )
            .toMutableList()
    }

    fun latest(type: String): TypeInfo? {
        val latest = latestVersion[type] ?: return null
        return metadata[TypeRefs.qualify(type, latest)]
    }
}
