package zincflow.core

import java.util.concurrent.ConcurrentHashMap

/** Holds the set of providers available to processors and tracks which
 * processors depend on which providers (for cascade-disable). One
 * context per Fabric instance; passed to [zincflow.fabric.ProcessorRegistry]
 * factories so processors can wire the providers they need at
 * construction time. */
class ProcessorContext {
    private val providers = ConcurrentHashMap<String, Provider>()
    private val dependents = ConcurrentHashMap<String, MutableList<String>>()

    fun addProvider(provider: Provider) {
        providers[provider.name()] = provider
    }

    fun getProvider(name: String?): Provider? {
        return providers[name]
    }

    @Suppress("UNCHECKED_CAST")
    fun <T : Provider> getProviderAs(name: String, type: Class<T>): T? {
        return providers[name].takeIf { type.isInstance(it) } as T?
    }

    fun listProviders(): List<String> {
        return providers.keys.toList()
    }

    fun providers(): Map<String, Provider> {
        return providers.toMap()
    }

    fun registerDependent(providerName: String, processorName: String) {
        dependents.computeIfAbsent(providerName) { mutableListOf() }
            .add(processorName)
    }

    fun getDependents(providerName: String): List<String> {
        return dependents[providerName]?.toList() ?: listOf()
    }

    fun shutdownAll() {
        for (p in providers.values) {
            if (p.isEnabled) p.disable(60)
            p.shutdown()
        }
    }
}
