package zincflow.core

/** SPI for third-party processors. Drop a JAR into the plugins directory
 * with a `META-INF/services/zincflow.core.ProcessorPlugin` entry
 * listing fully qualified class names, and the zinc-flow-java
 * [zincflow.fabric.PluginLoader] registers each one under its
 * [.type] with the [zincflow.fabric.Registry].
 * 
 * Implementations must have a public no-arg constructor —
 * [java.util.ServiceLoader] requires it. The [.create]
 * method does all the real work, with access to the pipeline's
 * [ProcessorContext] so plugins can pull shared infrastructure
 * (content store, logger, ...) the same way built-in processors do. */
interface ProcessorPlugin {
    /** Stable identifier used in `config.yaml` and
     * `/api/processors/add`. Must be unique across all registered
     * (type, version) pairs — a second plugin with the same type + version
     * overwrites the first (last loader wins). */
    fun type(): String

    /** Semver version for this processor. Exposed via
     * `GET /api/processor-types` so config authors can pin via
     * `type: MyProc@1.2.0`. Default `"1.0.0"` keeps
     * pre-versioning plugins compiling without changes. */
    fun version(): String {
        return "1.0.0"
    }

    /** Short description shown in the UI when browsing processor types. */
    fun description(): String {
        return ""
    }

    /** Config keys the processor accepts — surfaced to the UI so it
     * can render an "add processor" form. */
    fun configKeys(): MutableList<String> {
        return mutableListOf()
    }

    /** Result relationships this processor may produce
     * (e.g. [Relationships.SUCCESS], [Relationships.FAILURE],
     * [Relationships.MATCHED]). Used by the UI connection editor
     * to show the outbound ports. */
    fun relationships(): List<String> {
        return listOf(Relationships.SUCCESS, Relationships.FAILURE)
    }

    fun create(config: MutableMap<String, String>, context: ProcessorContext): Processor?
}
