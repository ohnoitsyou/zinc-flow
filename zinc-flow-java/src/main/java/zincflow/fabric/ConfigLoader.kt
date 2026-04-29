package zincflow.fabric

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml
import zincflow.core.Processor
import zincflow.core.ProcessorContext
import zincflow.core.Provider
import zincflow.core.Source
import zincflow.fabric.ConfigOverlay.Resolved
import zincflow.fabric.ConfigOverlay.load
import java.io.IOException
import java.nio.file.Path
import kotlin.Any
import kotlin.Throws
import kotlin.collections.ArrayList
import kotlin.collections.LinkedHashMap
import kotlin.collections.MutableList
import kotlin.collections.MutableMap
import kotlin.collections.get
import kotlin.collections.mutableListOf
import kotlin.require
import kotlin.requireNotNull
import kotlin.text.get
import kotlin.toString

/** Builds a [PipelineGraphKt] from a YAML config file.
 * 
 * Expected shape (mirrors zinc-flow-csharp):
 * 
 * <pre>
 * flow:
 * entryPoints: [ingress]
 * processors:
 * ingress:
 * type: LogAttribute
 * config:
 * prefix: "[in] "
 * router:
 * type: RouteOnAttribute
 * config:
 * routes: "high: priority == urgent"
 * connections:
 * ingress:
 * success: [router]
 * router:
 * high: [sink]
 * unmatched: [sink]
</pre> */
class ConfigLoader @JvmOverloads constructor(
    private val registry: Registry,
    context: ProcessorContext? = ProcessorContext(),
    private val sourceRegistry: SourceRegistry? = null,
    private val providerRegistry: ProviderRegistry? = null
) {
    /** Recorded shape of a processor definition (type + config) from the
     * last successful load. Keyed by processor name; consulted on the
     * next load to decide which instances can be reused. */
    class ProcessorSpec(val type: String, val config: MutableMap<String, String>) { }

    private val context: ProcessorContext = context ?: ProcessorContext()
    private var lastSpecs: MutableMap<String, ProcessorSpec> = mutableMapOf()
    private var lastProcessors: MutableMap<String, Processor> = mutableMapOf()
    private var lastOverlay: Resolved? = null
    private var lastSources = mutableListOf<Source>()
    private var lastProviders = mutableListOf<Provider>()

    fun context(): ProcessorContext {
        return context
    }

    /** Snapshot of the most recently loaded processor spec map. Callers
     * use this to diff the next load against the one that's currently
     * running — see [Pipeline.applyReload]. */
    fun lastSpecs(): MutableMap<String, ProcessorSpec> {
        return lastSpecs
    }

    /** The most recently loaded overlay stack — base + local + secrets,
     * the merged map, and per-key provenance. Used by
     * `GET /api/overlays`. */
    fun lastOverlay(): Resolved? {
        return lastOverlay
    }

    /** Sources constructed from the `sources:` block on the most
     * recent load. Callers wire these into the pipeline via
     * [Pipeline.addSource]; the loader does not start
     * them — that's [Pipeline.startSource]'s job. */
    fun lastSources(): MutableList<Source> {
        return lastSources
    }

    /** Providers constructed from the `providers:` block on the
     * most recent load. Empty when no block was present — in that
     * case the caller keeps the default provider set it bootstrapped. */
    fun lastProviders(): MutableList<Provider> {
        return lastProviders
    }

    @Throws(IOException::class)
    fun loadFromFile(path: Path?): PipelineGraphKt {
        val resolved: Resolved = load(path)
        return loadFromOverlay(resolved)
    }

    fun loadFromOverlay(resolved: Resolved): PipelineGraphKt {
        lastOverlay = resolved
        return load(resolved.effective)
    }

    fun load(yamlSource: String?): PipelineGraphKt {
        val parsed = Yaml().load<Any?>(yamlSource)
        require(parsed is MutableMap<*, *>) { "config: top-level must be a map" }
        return load(normalizeTop(parsed))
    }

    private fun load(effective: MutableMap<String, Any>): PipelineGraphKt {
        val flowRaw = effective["flow"]
        require(flowRaw is MutableMap<*, *>) { "config: missing 'flow' section" }

        // --- processors ---
        val procsRaw = flowRaw.get("processors")
        require(procsRaw is MutableMap<*, *>) { "config: 'flow.processors' must be a map" }
        val processors: MutableMap<String, Processor> = mutableMapOf()
        val specs: MutableMap<String, ProcessorSpec> = mutableMapOf()
        for (entry in procsRaw.entries) {
            val name: String = entry.key.toString()
            require(entry.value is MutableMap<*, *>) { "config: processor '" + name + "' must be a map" }
            val type: Any = def[TYPE_KEY]!!
            requireNotNull(type) { "config: processor '" + name + "' missing 'type'" }
            val config: MutableMap<String, String> = stringMap(def[CONFIG_KEY])
            val spec = ProcessorSpec(type.toString(), config)

            // Reuse the prior processor instance when the spec is
            // byte-identical — keeps in-flight state (counters, caches,
            // connections) across a reload instead of churning every
            // processor on a cosmetic config change.
            val p: Processor?
            val prior = lastSpecs.get(name)
            p = if (prior != null && prior == spec && lastProcessors.containsKey(name)) {
                lastProcessors[name]
            } else {
                registry.create(spec.type, spec.config, context)
            }
            processors[name] = p
            specs[name] = spec
        }

        // --- connections ---
        val connections: MutableMap<String, MutableMap<String, MutableList<String>>> = mutableMapOf()
//            HashMap<String, MutableMap<String, MutableList<String>>>()
        val connsRaw = flowRaw["connections"]
        if (connsRaw is MutableMap<*, *>) {
            for (fromEntry in connsRaw.entries) {
                val from: String = fromEntry.key.toString()
                require(fromEntry.value is MutableMap<*, *>) { "config: connections['" + from + "'] must be a map of relationship → targets" }
                val relationships: MutableMap<String, MutableList<String>> = mutableMapOf()
                for (relEntry in relationships.entries) {
                    val rel: String = relEntry.key
                    val targets: MutableList<String?> = stringList(relEntry.value)
                    relationships[rel] = targets
                }
                connections.put(from, relationships)
            }
        }

        // --- entry points ---
        val entryPoints: MutableList<String?> = stringList(flowRaw.get("entryPoints"))
        require(!entryPoints.isEmpty()) { "config: 'flow.entryPoints' must be a non-empty list" }
        for (ep in entryPoints) {
            require(processors.containsKey(ep)) { "config: entryPoint '" + ep + "' is not defined in processors" }
        }
        for (connEntry in connections.entries) {
            val from = connEntry.key
            require(processors.containsKey(from)) { "config: connection source '" + from + "' is not defined" }
        }

        // Full DAG check — accumulates every unknown-target error plus
        // cycle / unreachable warnings into one report. Errors trip a
        // single aggregate throw so the operator sees every issue at
        // once; warnings surface through the logger.
        val validation = FlowValidator.validate(processors.keys, connections)
        require(validation.errors.isEmpty()) {
            ("config: flow validation failed with " + validation.errors.size + " error(s):\n"
                    + String.join("\n", validation.errors))
        }
        for (warn in validation.warnings) {
            log.warn("flow warning: {}", warn)
        }

        // Commit the parsed spec + processor instances so the next
        // load can diff against them. Only happens after validation
        // succeeds — a partial load never corrupts the reload baseline.
        // Use an ordered unmodifiable view so spec iteration follows
        // declaration order (YAML round-trip relies on this).
        lastSpecs = specs
        lastProcessors = mutableMapOf(processors)
            Collections.unmodifiableMap<kotlin.String?, Processor?>(LinkedHashMap<kotlin.String?, Processor?>(processors))
        lastSources = buildSources(effective["sources"])
        lastProviders = buildProviders(effective.get("providers"))
        return PipelineGraphKt(processors, connections, entryPoints)
    }

    /** Build every provider declared under `providers:`. Same
     * `{type, config}` shape as processors and sources:
     * <pre>
     * providers:
     * logging:
     * type: LoggingProvider
     * prov:
     * type: ProvenanceProvider
     * config: {buffer: 10000}
    </pre> * 
     * Missing block → empty list, and the caller keeps whatever
     * provider set it bootstrapped. Factories returning null are
     * treated as "disabled for this config" — logged, not thrown. */
    private fun buildProviders(providersRaw: Any?): MutableList<Provider?> {
        if (providersRaw == null) return mutableListOf<Provider?>()
        require(providersRaw is MutableMap<*, *>) { "config: 'providers' must be a map of name → {type, config}" }
        if (providerRegistry == null) {
            log.warn("providers block present but no ProviderRegistry wired — providers ignored")
            return mutableListOf<Provider?>()
        }
        val out: MutableList<Provider?> = ArrayList<Provider?>()
        for (entry in providersRaw.entries) {
            val name: kotlin.String? = entry.key.toString()
            require(entry.value is MutableMap<*, *>) { "config: provider '" + name + "' must be a map with 'type' + 'config'" }
            val typeRaw: Any = def.get(TYPE_KEY)!!
            requireNotNull(typeRaw) { "config: provider '" + name + "' missing 'type'" }
            val providerConfig = if (def.get(CONFIG_KEY) is MutableMap<*, *>)
                stringKeyed(c)
            else
                Map.of<kotlin.String?, Any?>()
            val provider = providerRegistry.create(typeRaw.toString(), providerConfig)
            if (provider == null) {
                log.info("provider '{}' ({}): factory returned null — disabled", name, typeRaw)
                continue
            }
            out.add(provider)
        }
        return List.copyOf<Provider?>(out)
    }

    /** Build every source declared under `sources:`. Each entry
     * is a named block with `type:` (optionally versioned via
     * `@x.y.z`) and `config:`. Dispatch goes through the
     * [SourceRegistry], so third-party sources in plugin jars
     * become usable with zero changes here.
     * 
     * <pre>
     * sources:
     * infile:
     * type: GetFile@1.0.0
     * config:
     * inputDir: /var/spool/zincflow
     * pattern: "*.json"
     * heartbeat:
     * type: GenerateFlowFile
     * config:
     * content: "ping"
    </pre> * 
     * 
     * A null source returned by a factory (e.g. GetFile without
     * inputDir) is treated as "disabled" — logged, not thrown. */
    private fun buildSources(sourcesRaw: Any?): MutableList<Source?> {
        if (sourcesRaw == null) return mutableListOf()
        require(sourcesRaw is MutableMap<*, *>) { "config: 'sources' must be a map of name → {type, config}" }
        if (sourceRegistry == null) {
            log.warn("sources block present but no SourceRegistry wired — sources ignored")
            return mutableListOf()
        }
        val out: MutableList<Source> = mutableListOf()
        for (entry in sourcesRaw.entries) {
            val name: String = entry.key.toString()
            require(entry.value is MutableMap<*, *>) { "config: source '" + name + "' must be a map with 'type' + 'config'" }
            val typeRaw: Any = def[TYPE_KEY]!!
            val sourceConfig = if (false)
                stringKeyed(c)
            else
                mutableMapOf()
            val source = sourceRegistry.create(typeRaw.toString(), name, sourceConfig)
            if (source == null) {
                log.info("source '{}' ({}): factory returned null — disabled", name, typeRaw)
                continue
            }
            out.add(source)
        }
        return List.copyOf<Source?>(out)
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(ConfigLoader::class.java)

        /** Shared YAML keys for `{type, config}` blocks (processors,
         * sources, and — soon — providers). Also used by the admin HTTP
         * surface when it echoes the same shape back. */
        const val TYPE_KEY: String = "type"
        const val CONFIG_KEY: String = "config"

        private fun stringKeyed(raw: MutableMap<*, *>): MutableMap<String, Any> {
            return raw.mapNotNull { (k, v) -> v?.let { k.toString() to v } }.toMap().toMutableMap()
        }

        private fun stringMap(raw: Any?): MutableMap<String, String> {
            if (raw == null) return mutableMapOf()
            require(raw is MutableMap<*, *>) { "config: expected a map, got " + raw.javaClass.getSimpleName() }
            return raw.entries.associate { (k, v) -> "$k" to if(v == null) "" else "$v" }.toMutableMap()
        }

        private fun stringList(raw: Any?): MutableList<String> {
            if (raw == null) return mutableListOf()
            require(raw is MutableList<*>) { "config: expected a list, got " + raw.javaClass.getSimpleName() }
            return raw.map { it as String }.toMutableList()
        }

        /** Normalises top-level keys to String. SnakeYAML default loader
         * can yield `Map<Object, Object>`; downstream code expects
         * `Map<String, Object>`. */
        private fun normalizeTop(raw: MutableMap<*, *>): MutableMap<String, Any> {
            return raw.mapNotNull { (k, v) -> v?.let { k.toString() to v } }.toMap().toMutableMap()
        }
    }
}
