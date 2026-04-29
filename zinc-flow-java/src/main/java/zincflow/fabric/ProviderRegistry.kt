package zincflow.fabric

import zincflow.core.Provider
import java.util.List
import java.util.Map
import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiFunction

/** Registry of provider factories keyed by `name@version`.
 * Parallel to [Registry] / [SourceRegistry]. Callers
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
    fun interface Factory {
        fun create(config: MutableMap<String?, Any?>?): Provider?
    }

    class TypeInfo(
        val name: String?,
        val version: String?,
        val description: String?,
        configKeys: MutableList<String?>?
    ) {
        fun qualifiedName(): String {
            return TypeRefs.qualify(name, version)
        }

        val configKeys: MutableList<String?>?

        init {
            var configKeys = configKeys
            configKeys = List.copyOf<String?>(configKeys)
            this.configKeys = configKeys
        }
    }

    private val versioned = ConcurrentHashMap<String?, Factory?>()
    private val latestVersion = ConcurrentHashMap<String?, String?>()
    private val metadata = ConcurrentHashMap<String?, TypeInfo?>()

    fun register(info: TypeInfo, factory: Factory) {
        val key = info.qualifiedName()
        versioned.put(key, factory)
        metadata.put(key, info)
        latestVersion.merge(
            info.name, info.version!!,
            BiFunction { oldV: String?, newV: String? ->
                if (TypeRefs.compareVersions(
                        oldV,
                        newV
                    ) >= 0
                ) oldV else newV
            })
    }

    fun register(type: String?, factory: Factory) {
        register(TypeInfo(type, TypeRefs.DEFAULT_VERSION, "", List.of<String?>()), factory)
    }

    fun has(type: String?): Boolean {
        return resolveKey(type) != null
    }

    private fun resolveKey(type: String?): String {
        if (type == null || type.isEmpty()) return null
        if (type.contains("@")) return (if (versioned.containsKey(type)) type else null)!!
        val latest = latestVersion.get(type)
        return (if (latest == null) null else TypeRefs.qualify(type, latest))!!
    }

    fun create(type: String?, config: MutableMap<String?, Any?>?): Provider? {
        val key = resolveKey(type)
        requireNotNull(key) { "ProviderRegistry: unknown provider type '" + type + "'" }
        return versioned.get(key)!!.create(if (config == null) Map.of<String?, Any?>() else config)
    }

    fun listAll(): MutableList<TypeInfo?> {
        val out: MutableList<TypeInfo?> = ArrayList<TypeInfo?>(metadata.values())
        out.sortWith(
            Comparator.comparing<TypeInfo?, String?>(TypeInfo::name).thenComparing<String?>(
                        TypeInfo::version, Comparator { obj: String?, a: String? -> TypeRefs.compareVersions(a) })
        )
        return out
    }

    fun latest(type: String?): TypeInfo? {
        val latest = latestVersion.get(type)
        return if (latest == null) null else metadata.get(TypeRefs.qualify(type, latest))
    }
}
