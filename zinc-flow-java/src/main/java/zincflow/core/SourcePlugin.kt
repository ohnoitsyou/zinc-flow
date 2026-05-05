package zincflow.core

/** SPI for third-party sources. Drop a JAR into the plugin directory
 * that exposes a `SourcePlugin` service and the source becomes
 * usable from `config.yaml` under the `sources:` block,
 * the same way processors do under `flow.processors:`.
 * 
 * Convention: [.create] does not start the source;
 * lifecycle is [zincflow.fabric.Pipeline.startSource]'s job. */
interface SourcePlugin {
    /** Stable identifier used in `config.yaml`. Must be unique
     * across all registered (type, version) pairs. */
    fun sourceType(): String

    /** Semver version for this source. Exposed via
     * `GET /api/source-types` so config authors can pin via
     * `type: MySource@1.2.0`. Default `"1.0.0"`. */
    fun version(): String {
        return "1.0.0"
    }

    /** Short description shown in the UI. */
    fun description(): String {
        return ""
    }

    /** Config keys the source accepts — surfaced to the UI so it can
     * render an "add source" form. */
    fun configKeys(): List<String> {
        return listOf()
    }

    /** Instantiate the source given its config-file name and config
     * map. `name` is the key under `sources:` in YAML —
     * the source uses it as its own [Source.name]. */
    fun create(name: String, config: Map<String, Any>): Source?
}
