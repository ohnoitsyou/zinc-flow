package zincflow.core

/** Shared lifecycle contract for providers, implemented by the built-in
 * ConfigProvider, LoggingProvider, ProvenanceProvider, and
 * ContentProvider. User code can implement to expose custom shared
 * infrastructure (metrics sinks, cache pools, schema registries).
 * Mirrors zinc-flow-csharp's IProvider. */
interface Provider {
    /** Runtime identifier used to look the provider up in a
     * [ProcessorContext]. Convention: lowercase, one word
     * (e.g. `"config"`, `"logging"`, `"content"`).
     * Exposed as `NAME` on every built-in provider. */
    fun name(): String

    /** Registry-facing type identifier — the string a config file
     * puts under `type:` and that [ProviderPlugin] uses
     * to key its factory. Convention: CamelCase class-style
     * (e.g. `"LoggingProvider"`, `"ContentProvider"`).
     * Exposed as `TYPE` on every built-in provider.
     * 
     * 
     * Two identifiers is intentional: `name()` is the
     * instance-local key ("which provider in this context"), while
     * `providerType()` is the factory-level key ("what kind
     * of provider is this"). */
    fun providerType(): String

    fun state(): ComponentState

    fun enable()

    fun disable(drainTimeoutSeconds: Int)

    fun shutdown()

    val isEnabled: Boolean
        get() = state() == ComponentState.ENABLED
}
