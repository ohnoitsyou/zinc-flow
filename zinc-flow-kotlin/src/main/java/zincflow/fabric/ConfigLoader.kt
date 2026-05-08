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
import kotlin.time.TimeSource


/** Builds a [PipelineGraph] from a YAML config file.
 * 
 * Expected shape (mirrors zinc-flow-csharp):
 * 
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
 */
/*

flow_defn:
  - entryPoints: <entryPoints_defn>
  - processors: <processors_defn>
  - connections: <connections_defn>

processors_defn: <Map<processor_defn>>
processor_defn: <Map<String, Map<String, String>>>
 - key: String
 - value: Map<String, String>

flow: 
  entryPoints:
    - a
  processors:
    a:
      type: UpdateAttribute
      config:
        key: stage
        value: first
    b:
      type: LogAttribute
  connections:
    a:
      success: [b]
 */
class ConfigProcessingException(msg: String, ex: Throwable? = null) : RuntimeException(msg, ex)
data class ProcessorSpec(val type: String, val config: Map<String, String>)

class ConfigLoader @JvmOverloads constructor(
    private val registry: Registry,
    context: ProcessorContext? = ProcessorContext(),
    private val sourceRegistry: SourceRegistry? = null,
    private val providerRegistry: ProviderRegistry? = null,
) {
    /** Recorded shape of a processor definition (type + config) from the
     * last successful load. Keyed by processor name; consulted on the
     * next load to decide which instances can be reused. */

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
    fun lastSpecs(): Map<String, ProcessorSpec> {
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
    fun loadFromFile(path: Path?): PipelineGraph {
        val resolved: Resolved = load(path)
        return loadFromOverlay(resolved)
    }

    fun loadFromOverlay(resolved: Resolved): PipelineGraph {
        lastOverlay = resolved
        log.info("##### Load From Overlay: ${resolved.effective} #####")
        return load(resolved.effective)
    }

    fun load(yamlSource: String?): PipelineGraph {
//        val mapper = ObjectMapper(YAMLFactory.builder().configure(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION, true).build()).findAndRegisterModules()
//        val w = mapper.readValue(yamlSource, FlowWrapper::class.java)
//        loadFromObjectMapper(w)

        val parsed = Yaml().load<Any?>(yamlSource)
        require(parsed is MutableMap<*, *>) { "config: top-level must be a map" }
        val top = normalizeTop(parsed)
        return load(top)
    }

    private fun loadFromObjectMapper(flowWrapper: FlowWrapper): PipelineGraph {
        val validationResult = FlowValidator.validate(flowWrapper.flow.processors.keys, flowWrapper.flow.connections)
        require(validationResult.errors.isEmpty()) {
            "config: flow validation failed with ${validationResult.errors.size} error(s):\n" + validationResult.errors.joinToString("\n")
        }
        for (warn in validationResult.warnings) {
            log.warn("flow warning: $warn")
        }

        flowWrapper.flow.processors.map {

        }

        return PipelineGraph(mapOf(), mapOf(), listOf(), 0)
    }

    private fun instantiateProcessors(processors: Map<String, ConfigProcessor>) {
        val createdProcessors = mutableMapOf<String, Processor>()
        val specs = mutableMapOf<String, ProcessorSpec>()
        processors.forEach { (name, processor) ->
            val spec = ProcessorSpec(processor.type, processor.config)
            val last = lastSpecs[name]
            val proc: Processor = if (last != null && last == spec && lastProcessors.containsKey(name)) {
                lastProcessors[name]!!
            } else {
                registry.create(spec.type, spec.config, context)!!
            }
            createdProcessors[name] = proc
            specs[name] = spec
        }
    }

    private fun load(effective: Map<String, Any>): PipelineGraph {
        val appStartTime = TimeSource.Monotonic.markNow()
        log.info("##### Starting configuration load: $appStartTime #####")
        val flowRaw = effective["flow"] as? Map<*, *> ?: throw ConfigProcessingException("config: missing 'flow' section")

        // --- processors ---
        log.info("Loading processors")
        val procsRaw = flowRaw["processors"] as? Map<*, *> ?: throw ConfigProcessingException("config: 'flow.processors' must be a map")
        val processors: MutableMap<String, Processor> = mutableMapOf()
        val specs: MutableMap<String, ProcessorSpec> = mutableMapOf()
        for (entry in procsRaw.entries) {
            val name: String = entry.key as String
            val processor = entry.value as? Map<*, Any?> ?: throw ConfigProcessingException("config: processor '$name' must be a map" )
            val type = processor[TYPE_KEY]?.toString() ?: throw ConfigProcessingException("config: processor '$name' missing 'type'")
            val config = stringMap(processor[CONFIG_KEY])
            val spec = ProcessorSpec(type, config)

            // Reuse the prior processor instance when the spec is
            // byte-identical — keeps in-flight state (counters, caches,
            // connections) across a reload instead of churning every
            // processor on a cosmetic config change.
            val prior = lastSpecs[name]
            val p = if (prior != null && prior == spec && lastProcessors.containsKey(name)) {
                lastProcessors[name]!!
            } else {
                registry.create(spec.type, spec.config, context) ?: throw ConfigProcessingException("config: Unknown procesor type: '${spec.type}")
            }
            processors[name] = p
            specs[name] = spec
        }
        log.info("Done processors: Loaded ${processors.size} processor(s)")

        // --- connections ---
        log.info("Loading connections")
        val connections: MutableMap<String, MutableMap<String, List<String>>> = mutableMapOf()
//            HashMap<String, MutableMap<String, MutableList<String>>>()
        val connsRaw = flowRaw["connections"]
        if (connsRaw is MutableMap<*, *>) {
            for (fromEntry in connsRaw.entries) {
                val from: String = fromEntry.key.toString()
                val entryCons = fromEntry.value as? Map<*, *>  ?: throw ConfigProcessingException("config: connections['$from'] must be a map of relationship → targets" )
//                require(entryCons is Map<*, *>) { "config: connections['$from'] must be a map of relationship → targets" }
                val relationships: MutableMap<String, List<String>> = mutableMapOf()
                for (relEntry in entryCons.entries) {
                    val rel: String = relEntry.key as String
                    val targets: List<String> = stringList(relEntry.value)
                    relationships[rel] = targets
                }
                connections[from] = relationships
            }
        }
        log.info("Loaded connections: Loaded ${connections.size} connection(s)")

        // --- entry points ---
        log.info("Loading entry points")
        val entryPoints: List<String> = stringList(flowRaw["entryPoints"])
        if(entryPoints.isEmpty()) throw ConfigProcessingException("config: 'flow.entryPoints' must be a non-empty list" )
//        require(!entryPoints.isEmpty()) { "config: 'flow.entryPoints' must be a non-empty list" }
        entryPoints.filterNot { processors.containsKey(it) }.takeIf { it.isNotEmpty() }?.let { e ->
            throw ConfigProcessingException(e.joinToString("\n") { "config: entryPoint '$it' is not defined in processors" })
        }
        log.info("Loaded entry points: Loaded ${entryPoints.size} entry point(s)")

//        for (ep in entryPoints) {
//            check(processors.containsKey(ep)) { "config: entryPoint '$ep' is not defined in processors" }
//        }
        connections.keys.filterNot { processors.containsKey(it) }.takeIf { it.isNotEmpty() }?.let { e ->
            throw ConfigProcessingException(e.joinToString("\n") { "config: connection source '$it' is not defined" })
        }
//        for (connEntry in connections.entries) {
//            val from = connEntry.key
//            require(processors.containsKey(from)) { "config: connection source '$from' is not defined" }
//        }

        // Full DAG check — accumulates every unknown-target error plus
        // cycle / unreachable warnings into one report. Errors trip a
        // single aggregate throw so the operator sees every issue at
        // once; warnings surface through the logger.
        log.info("Starting flow validation")
        val validation = FlowValidator.validate(processors.keys, connections)
        if(validation.errors.isNotEmpty()) {
            throw ConfigProcessingException("config: flow validation failed with ${validation.errors.size} error(s):\n ${validation.errors.joinToString("\n")}")
        }
//        require(validation.errors.isEmpty()) {
//            ("config: flow validation failed with ${validation.errors.size} error(s):\n"
//                    + validation.errors.joinToString("\n"))
//        }
        for (warn in validation.warnings) {
            log.warn("flow warning: {}", warn)
        }
        log.info("Flow validation complete: is valid: ${validation.ok()}")

        // Commit the parsed spec + processor instances so the next
        // load can diff against them. Only happens after validation
        // succeeds — a partial load never corrupts the reload baseline.
        // Use an ordered unmodifiable view so spec iteration follows
        // declaration order (YAML round-trip relies on this).
        lastSpecs = specs

        lastProcessors.clear()
        lastProcessors.putAll(processors)
//            Collections.unmodifiableMap<kotlin.String?, Processor?>(LinkedHashMap<kotlin.String?, Processor?>(processors))

        log.info("Building sources")
        lastSources = buildSources(effective["sources"])
        log.info("Sources complete")

        log.info("Loading providers")
        lastProviders = buildProviders(effective["providers"])
        log.info("Providers complete")

        val elapsed = appStartTime.elapsedNow()
        log.info("##### Pipeline processing complete: ${elapsed.inWholeMilliseconds} ms #####")
        return PipelineGraph(processors, connections, entryPoints)
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
    private fun buildProviders(providersRaw: Any?): MutableList<Provider> {
        if (providersRaw == null) return mutableListOf()
        require(providersRaw is MutableMap<*, *>) { "config: 'providers' must be a map of name → {type, config}" }
        if (providerRegistry == null) {
            log.warn("providers block present but no ProviderRegistry wired — providers ignored")
            return mutableListOf()
        }
        val out = mutableListOf<Provider>()
        for (entry in providersRaw.entries) {
            val name: String = entry.key.toString()
            val entryValue = entry.value
            require(entryValue is MutableMap<*, *>) { "config: provider '$name' must be a map with 'type' + 'config'" }
            val typeRaw = entryValue[TYPE_KEY]
            requireNotNull(typeRaw) { "config: provider '$name' missing 'type'" }
            val entryConfig = entryValue[CONFIG_KEY]
            val providerConfig = if (entryConfig is MutableMap<*, *>) {
                stringKeyed(entryConfig)
            } else {
                mutableMapOf()
            }
            val provider = providerRegistry.create(typeRaw.toString(), providerConfig)
            if (provider == null) {
                log.info("provider '{}' ({}): factory returned null — disabled", name, typeRaw)
                continue
            }
            out.add(provider)
        }
        return out.toMutableList()
    }

    /** Build every source declared under `sources:`. Each entry
     * is a named block with `type:` (optionally versioned via
     * `@x.y.z`) and `config:`. Dispatch goes through the
     * [SourceRegistry], so third-party sources in plugin jars
     * become usable with zero changes here.
     * 
     * <pre>
     * sources:
     *   infile:
     *     type: GetFile@1.0.0
     *     config:
     *       inputDir: /var/spool/zincflow
     *       pattern: "*.json"
     *   heartbeat:
     *     type: GenerateFlowFile
     *     config:
     *       content: "ping"
    </pre> * 
     * 
     * A null source returned by a factory (e.g. GetFile without
     * inputDir) is treated as "disabled" — logged, not thrown. */
    private fun buildSources(sourcesRaw: Any?): MutableList<Source> {
        log.info("Starting build sources")
        log.info(sourcesRaw.toString())
        if (sourceRegistry == null) {
            log.warn("sources block present but no SourceRegistry wired — sources ignored")
            return mutableListOf()
        }

        if (sourcesRaw == null) return mutableListOf()
        require(sourcesRaw is Map<*, *>) { "config: 'sources' must be a map of name → {type, config}" }
        log.info("Processing sources: ${sourcesRaw.entries.size}")

        val out: MutableList<Source> = mutableListOf()
        for (entry in sourcesRaw.entries) {
            log.info("Source: ${entry.key}")
            val name: String = entry.key.toString()
            val sourceRoot = entry.value
            require(sourceRoot is Map<*, *>) { "config: sources: section for '$name' did not parse as Map" }

            val typeRaw = sourceRoot[TYPE_KEY]
            if(typeRaw !is String) {
                log.warn("Source defined '$name' but no type found - skipping")
                continue
            }

            val configRaw = sourceRoot[CONFIG_KEY]
            val sourceConfig = if (configRaw is MutableMap<*, *>) {
                stringKeyed(configRaw)
            } else {
                mutableMapOf()
            }
            log.info("Source config: $sourceConfig")

            val source = sourceRegistry.create(typeRaw, name, sourceConfig)
            if (source == null) {
                log.info("source '$name' ($typeRaw): factory returned null — disabled", name, typeRaw)
                continue
            }

            out.add(source)
            log.info("Done with source: $name")
        }
        return out.toMutableList()
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

        private fun stringMap(raw: Any?): Map<String, String> {
            if (raw == null) return mapOf()
            require(raw is Map<*, *>) { "config: expected a map, got " + raw.javaClass.getSimpleName() }
            return raw.entries.associate { (k, v) -> "$k" to if(v == null) "" else "$v" }
        }

        private fun stringList(raw: Any?): List<String> {
            if (raw == null) return listOf()
            require(raw is List<*>) { "config: expected a list, got " + raw.javaClass.getSimpleName() }
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

/**
 * The root wrapper for the YAML document.
 */
data class FlowWrapper(
    val flow: FlowDefinition,
    val sources: Map<String, ConfigSource>
)

/**
 * Defines the structure of the flow, including entry points,
 * processor definitions, and their connections.
 */
data class FlowDefinition(
    val entryPoints: List<String>,
    val processors: Map<String, ProcessorSpec>,
    val connections: Map<String, Map<String, List<String>>>,
)

/**
 * Represents an individual processor unit.
 * 'config' is a Map to allow for varying configurations based on the processor type.
 */
data class ConfigProcessor (
    val type: String,
    val config: Map<String, String> = mapOf(),
)

data class ConfigSource(val type: String, val config: Map<String, Any> = mapOf())
