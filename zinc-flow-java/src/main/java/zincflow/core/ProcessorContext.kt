package zincflow.core

import java.util.concurrent.ConcurrentHashMap

/** Holds the set of providers available to processors and tracks which
 * processors depend on which providers (for cascade-disable). One
 * context per Fabric instance; passed to [zincflow.fabric.Registry]
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

    fun listProviders(): MutableList<String> {
        return providers.keys.toMutableList()
    }

    fun providers(): MutableMap<String, Provider> {
        return providers.toMutableMap()
    }

    fun registerDependent(providerName: String, processorName: String) {
        dependents.computeIfAbsent(providerName) { _ : String -> mutableListOf() }
            .add(processorName)
    }

    fun getDependents(providerName: String): MutableList<String> {
        return dependents[providerName]?.toMutableList() ?: mutableListOf()
    }

    fun shutdownAll() {
        for (p in providers.values) {
            if (p.isEnabled) p.disable(60)
            p.shutdown()
        }
    }
}
