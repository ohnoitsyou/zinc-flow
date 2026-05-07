package zincflow.core

/** SPI for providers. Plugins surface shared infrastructure (metrics
 * sinks, schema registries, external caches, version control) through
 * the same [ProcessorContext] that built-in providers live in,
 * so processor plugins can require them the same way built-in
 * processors do.
 * 
 * Every built-in provider ships a `ProviderPlugin` through
 * `META-INF/services/zincflow.core.ProviderPlugin` — that's the
 * single discovery path for the full provider set. A plugin jar that
 * registers a higher-versioned entry under the same
 * [.providerType] replaces the built-in at latest-version
 * lookup.
 * 
 * Convention: [.create] returns the provider already
 * constructed but not yet enabled; the boot path calls
 * [Provider.enable] after inserting into the context.
 * Returning `null` means "disabled for this config" — e.g.
 * `UIRegistrationProvider` factories return null when
 * `ui.register_to` is absent. */
interface ProviderPlugin {
    /** Stable identifier — maps to the `type:` field under a
     * `providers:` block in config.yaml. Must be unique across
     * registered (type, version) pairs. */
    fun providerType(): String

    /** Semver version. Default `"1.0.0"`. A plugin jar with a
     * higher version than the built-in of the same type replaces the
     * built-in at unqualified lookup. */
    fun version(): String {
        return "1.0.0"
    }

    /** Short description shown in the UI. */
    fun description(): String {
        return ""
    }

    /** Config keys the provider accepts — surfaced to admin tooling. */
    fun configKeys(): List<String> {
        return listOf()
    }

    /** Instantiate the provider from its config. Return `null`
     * when the provider should be skipped (e.g. a conditional provider
     * whose enabling config key is absent). Return a non-null
     * [Provider] to have it added to the context and enabled. */
    fun create(config: Map<String, Any>): Provider?
}
