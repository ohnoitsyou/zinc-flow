package zincflow

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorContext
import zincflow.core.Provider
import zincflow.core.Relationships
import zincflow.fabric.ConfigLoader
import zincflow.fabric.FlowValidator
import zincflow.fabric.HttpServer
import zincflow.fabric.Metrics
import zincflow.fabric.NodeIdentity
import zincflow.fabric.Pipeline
import zincflow.fabric.PipelineGraph
import zincflow.fabric.PluginLoader
import zincflow.fabric.ProviderRegistry
import zincflow.fabric.Registry
import zincflow.fabric.SourceRegistry
import zincflow.fabric.TypeRefs
import zincflow.processors.LogAttribute
import zincflow.processors.RouteOnAttribute
import zincflow.processors.UpdateAttribute
import zincflow.providers.UIRegistrationProvider
import zincflow.providers.VersionControlProvider
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicReference
import kotlin.system.exitProcess

/** Entry point. Loads a config.yaml if one is present (arg 1 or
 * ./config.yaml), otherwise falls back to a built-in demo pipeline so
 * `zinc run` with no config still produces something useful. */
object Zinc {
    private val log: Logger = LoggerFactory.getLogger(Zinc::class.java)

    // --- Config key paths (relative to the effective layered map) -------
    private const val CFG_UI = "ui"
    private const val CFG_UI_REGISTER = "register_to"
    private const val CFG_VC = "vc"
    private const val CFG_VC_ENABLED = "enabled"
    private const val CFG_VC_REPO = "repo"
    private const val CFG_VC_GIT = "git"
    private const val CFG_VC_REMOTE = "remote"
    private const val CFG_VC_BRANCH = "branch"

    // --- Runtime env / system properties --------------------------------
    private const val ENV_PLUGINS_DIR = "ZINCFLOW_PLUGINS_DIR"
    private const val PROP_PLUGINS_DIR = "zincflow.pluginsDir"
    private const val PROP_PORT = "zincflow.port"
    private const val DEFAULT_PORT = "9092"

    @Throws(IOException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        // Subcommand dispatch — mirrors zinc-flow-csharp's
        // Program.cs top-of-main switch.
        if (args.isNotEmpty()) {
            val mode = args[0]
            when (mode) {
                "validate", "--validate" -> {
                    require(args.size > 1) { " path to configuration not provided"}
                    val exit = runValidate(args[1])
                    exitProcess(exit)
                }

                "bench", "--bench" -> {
                    runBench()
                    return
                }

                "help", "--help", "-h" -> {
                    printUsage()
                    return
                }

                else -> {}
            }
        }

        val configPath = resolveConfigPath(args)
        val registry = Registry()
        val sourceRegistry = SourceRegistry()
        val providerRegistry = ProviderRegistry()
        val context = ProcessorContext()

        // Populate registries via ServiceLoader — same path plugin jars
        // use, so built-ins and plugins are discovered uniformly.
        PluginLoader.loadSources(Zinc::class.java.classLoader, sourceRegistry)
        PluginLoader.loadProviders(Zinc::class.java.classLoader, providerRegistry)

        // Register the two bootstrap-dependent providers (identity
        // supplier, resolved repo path) as registry factories that
        // close over Main-local state. An AtomicReference lets the
        // UIReg factory run before identity is resolved — the supplier
        // is called lazily at heartbeat time, not construct time.
        val identityRef = AtomicReference<NodeIdentity>()
        registerBootstrapProviders(providerRegistry, identityRef, configPath)

        // Plugin discovery — scan $ZINCFLOW_PLUGINS_DIR (default ./plugins)
        // for third-party processor/provider jars before loading the flow,
        // so config.yaml can reference plugin-provided types.
        val pluginsDir = resolvePluginsDir()
        val plugins = PluginLoader.loadFromDirectory(
            pluginsDir, registry, context, sourceRegistry
        )
        if (plugins.totalLoaded() > 0) {
            log.info(
                "loaded {} plugin(s) from {} — providers: {}, processors: {}, sources: {}",
                plugins.totalLoaded(), pluginsDir, plugins.providerNames,
                plugins.processorTypes, plugins.sourceTypes
            )
        }

        val loader = ConfigLoader(registry, context, sourceRegistry, providerRegistry)
        val graph: PipelineGraph?
        if (configPath != null && Files.isRegularFile(configPath)) {
            log.info("loading pipeline from {}", configPath.toAbsolutePath())
            graph = loader.loadFromFile(configPath)
        } else {
            log.info("no config.yaml found — using built-in demo pipeline")
            graph = demoGraph()
        }
        val metrics = Metrics()
        val pipeline = Pipeline(graph, Pipeline.DEFAULT_MAX_HOPS, metrics, context, registry)

        // Backfill processorDefs with the YAML-loaded configs so the
        // UI drawer / /api/flow can surface them. addProcessor (the
        // HTTP mutation path) already calls recordProcessorDef, so
        // this is only needed for the initial YAML-driven population.
        for (entry in loader.lastSpecs().entries) {
            val spec = entry.value
            pipeline.recordProcessorDef(entry.key, spec.type, spec.config, mutableListOf())
        }

        // Resolve node identity from the effective layered config (or
        // the persisted UUID file), then populate the atomic so the
        // UIReg factory's lazy identity supplier can see it.
        val effective: MutableMap<String, Any> = if (loader.lastOverlay() == null) {
            mutableMapOf()
        } else {
            loader.lastOverlay()?.effective ?: mutableMapOf()
        }
        val identity = NodeIdentity.resolve(
            effective,
            Path.of(NodeIdentity.NODE_ID_FILE),
            Zinc::class.java.getPackage().implementationVersion //?: "1.0.0"
        )
        identityRef.set(identity)

        // Wire providers. The providers: block in config — if present —
        // takes precedence. Otherwise, we instantiate every registered
        // type with its config map drawn from the effective overlay,
        // which preserves the default set (logging, config, provenance,
        // content, schema_registry) plus conditional ones (UIReg when
        // ui.register_to is set, VC when vc.enabled is true).
        val providersToWire = if (loader.lastProviders().isEmpty()) {
            defaultProviders(providerRegistry, effective)
        } else {
            loader.lastProviders()
        }
        for (p in providersToWire) {
            context.addProvider(p)
            p.enable()
            log.info("provider {} ({}) enabled", p.name(), p.providerType())
        }

        // Register + auto-start every source configured under
        // sources:. Sources emit FlowFiles into pipeline.ingest() the
        // same way HTTP POST / does, so the graph's entryPoints see
        // them indistinguishably.
        for (source in loader.lastSources()) {
            pipeline.addSource(source)
            pipeline.startSource(source.name())
            log.info("source {} ({}) started", source.name(), source.sourceType())
        }

        val port = resolvePort()
        val server = HttpServer(pipeline, loader, configPath, plugins, pluginsDir, identity).start(port)
        log.info("zinc-flow-java up — node ${identity.nodeId} at http://localhost:${server.port()} (hostname ${identity.hostname})")
        log.info("dashboard: GET http://localhost:${server.port()}/dashboard metrics: /metrics")
        registerShutdownHook(server, pipeline, context)
    }

    /** Stop sources, drop the HTTP socket, and quiesce providers on
     * SIGTERM. Runs on a dedicated platform thread — `shutdown()`
     * implementations must be fast and non-blocking (they already are). */
    private fun registerShutdownHook(server: HttpServer, pipeline: Pipeline, context: ProcessorContext) {
        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().name("zinc-flow-shutdown").unstarted {
            log.info("shutdown signal received — stopping worker")
            for (source in pipeline.sources().values) {
                try {
                    source.stop()
                } catch (ex: RuntimeException) {
                    log.warn("source {} stop failed: {}", source.name(), ex.toString())
                }
            }
            try {
                server.stop()
            } catch (ex: RuntimeException) {
                log.warn("server stop failed: {}", ex.toString())
            }
            try {
                context.shutdownAll()
            } catch (ex: RuntimeException) {
                log.warn("provider shutdown failed: {}", ex.toString())
            }
            log.info("worker stopped cleanly")
        })
    }

    private fun resolvePort(): Int {
        return System.getProperty(PROP_PORT, DEFAULT_PORT).toInt()
    }

    /** Register the two providers whose construction depends on
     * Main-local state (node identity, config file location) as
     * closures over that state. Done before config load so the
     * `providers:` block can reference them. */
    private fun registerBootstrapProviders(
        providerRegistry: ProviderRegistry,
        identityRef: AtomicReference<NodeIdentity>,
        configPath: Path?
    ) {
        providerRegistry.register(
            ProviderRegistry.TypeInfo(
                UIRegistrationProvider.TYPE,
                TypeRefs.DEFAULT_VERSION,
                "Self-registers this worker with a central UI via periodic heartbeat.",
                mutableListOf(CFG_UI_REGISTER)
            )) { cfg: Map<String, Any> ->
                val target = cfg[CFG_UI_REGISTER]
                if (target == null || target.toString().isEmpty()) return@register null
                UIRegistrationProvider(target.toString(), { identityRef.get().toMap(resolvePort()) })
            }

        providerRegistry.register(
            ProviderRegistry.TypeInfo(
                VersionControlProvider.TYPE, TypeRefs.DEFAULT_VERSION,
                "Shells out to system git for flow-config commit + push.",
                mutableListOf(CFG_VC_ENABLED, CFG_VC_REPO, CFG_VC_GIT, CFG_VC_REMOTE, CFG_VC_BRANCH)
            )) { cfg: Map<String, Any> ->
                val enabled = cfg[CFG_VC_ENABLED]
                val on = enabled as? Boolean ?: enabled.toString().toBoolean()
                if (!on) return@register null
                val repo = if (cfg.containsKey(CFG_VC_REPO)) {
                    Path.of(cfg[CFG_VC_REPO].toString())
                } else {
                    if (configPath == null) Path.of(".") else configPath.toAbsolutePath().parent
                }
                VersionControlProvider(
                    repo,
                    cfg.getOrDefault(CFG_VC_GIT, "git").toString(),
                    cfg.getOrDefault(CFG_VC_REMOTE, "origin").toString(),
                    cfg.getOrDefault(CFG_VC_BRANCH, "main").toString()
                )
            }
    }

    /** Build the default provider set when `providers:` is absent
     * from config. For every registered provider type we call its
     * factory with whatever matches the conventional config key
     * (e.g. the `ui` / `vc` sub-maps for UIReg / VC) or
     * an empty map for the stateless built-ins. Null factory returns
     * mean "disabled" and are filtered out. */
    private fun defaultProviders(
        providerRegistry: ProviderRegistry,
        effective: MutableMap<String, Any>
    ): List<Provider> {
        return providerRegistry.listAll().mapNotNull { info ->
            val cfg = when (info.name) {
                UIRegistrationProvider.TYPE if (effective[CFG_UI] is Map<*, *>) -> effective[CFG_UI] as Map<String, Any>
                VersionControlProvider.TYPE if (effective[CFG_VC] is Map<*, *>) -> effective[CFG_VC] as Map<String, Any>
                else -> mapOf()
            }
            providerRegistry.create(info.qualifiedName(), cfg)
        }
    }

    private fun resolveConfigPath(args: Array<String>): Path? {
        if (args.isNotEmpty()) return Path.of(args[0])
        val defaultPath = Path.of("config.yaml")
        return if (Files.isRegularFile(defaultPath)) defaultPath else null
    }

    /** Pick the plugins directory — `$ZINCFLOW_PLUGINS_DIR` when
     * set, otherwise `./plugins`. Non-existent paths are fine;
     * [PluginLoader.loadFromDirectory] returns an empty summary
     * in that case so the CLI path doesn't hard-fail on a fresh
     * checkout that hasn't shipped plugins yet. */
    private fun resolvePluginsDir(): Path {
        val envDir = System.getenv(ENV_PLUGINS_DIR)
        if (envDir != null && !envDir.isEmpty()) return Path.of(envDir)
        val propDir = System.getProperty(PROP_PLUGINS_DIR)
        if (propDir != null && !propDir.isEmpty()) return Path.of(propDir)
        return Path.of("plugins")
    }

    /** Built-in demo pipeline used when no config.yaml is provided.
     * Mirrors the Phase 2 hard-coded graph. */
    fun demoGraph(): PipelineGraph {
        val ingress: Processor = LogAttribute("[ingress] ")
        val router: Processor = RouteOnAttribute("high: priority == urgent; low: priority == normal")
        val elevate: Processor = UpdateAttribute("priority", "elevated")
        val tail: Processor = LogAttribute("[tail] ")

        val processors = mutableMapOf(
            "ingress" to ingress,
            "router" to router,
            "elevate" to elevate,
            "tail" to tail
        )
        val connections = mutableMapOf(
            "ingress"  to mutableMapOf(Relationships.SUCCESS to mutableListOf("router")),
            "router" to mutableMapOf(
                "high" to mutableListOf("elevate"),
                "low" to mutableListOf("tail"),
                Relationships.UNMATCHED to mutableListOf("tail")
            ),
            "elevate" to mutableMapOf(Relationships.SUCCESS to mutableListOf("tail"))
        )
        return PipelineGraph(processors, connections, listOf("ingress"))
    }

    /** Static validation without starting the worker. Exit codes match
     * zinc-flow-csharp: 0 = clean, 1 = errors, 2 = file not found.
     * Warnings are reported but don't affect the exit code. */
    @JvmStatic
    fun runValidate(pathArg: String): Int {
        val path = Path.of(pathArg)
        if (!Files.isRegularFile(path)) {
            System.err.println("validate: config file not found: " + path.toAbsolutePath())
            return 2
        }

        val registry = Registry()
        val sources = SourceRegistry()
        val providers = ProviderRegistry()
        PluginLoader.loadSources(Zinc::class.java.classLoader, sources)
        PluginLoader.loadProviders(Zinc::class.java.classLoader, providers)
        val loader = ConfigLoader(registry, ProcessorContext(), sources, providers)

        val graph: PipelineGraph?
        try {
            graph = loader.loadFromFile(path)
        } catch (ex: Exception) {
            System.err.println("validate: " + path + " — " + ex.message)
            return 1
        }

        val result =
            FlowValidator.validate(graph.processors.keys, graph.connections)

        println("validate: $path")
        if (result.errors.isEmpty() && result.warnings.isEmpty()) {
            println("  no issues — config is valid")
            return 0
        }
        result.errors.forEach { println("  [error]   $it") }
        result.warnings.forEach { println("  [warning]   $it") }
        println()
        println("summary: ${result.errors.size} error(s), ${result.warnings.size} warning(s)")
        return if (result.errors.isEmpty()) 0 else 1
    }

    /** Throughput benchmark — two-hop UpdateAttribute pipeline, no HTTP,
     * no providers. Prints ff/s at several payload counts so Java and
     * C# track runs are directly comparable. Mirrors zinc-flow-csharp's
     * RunBenchmarks in Program.cs:332. */
    @JvmStatic
    fun runBench() {
        println("=== zinc-flow-java benchmark ===")
        println("Runtime: ${System.getProperty("java.vm.name")}  ${System.getProperty("java.runtime.version")}")
        println()

        println("Warmup (JIT)...")
        benchThroughput(10000, quiet = true)

        System.gc() // best-effort pre-measurement settle
        println()
        println("Pipeline throughput (2-hop direct execution):")
        benchThroughput(10000, false)
        benchThroughput(50000, false)
        benchThroughput(100000, false)
        benchThroughput(500000, false)
    }

    private fun benchThroughput(n: Int, quiet: Boolean) {
        val tag: Processor = UpdateAttribute("env", "prod")
        val sink: Processor = UpdateAttribute("done", "true")
        val processors = mutableMapOf("tag" to tag, "sink" to sink)
        val connections = mutableMapOf(
            "tag" to mutableMapOf(Relationships.SUCCESS to mutableListOf("sink"))
        )
        val graph = PipelineGraph(processors, connections, listOf("tag"))
        val pipeline = Pipeline(graph)

        val payload = "bench payload data here".toByteArray(StandardCharsets.UTF_8)
        val start = System.nanoTime()
        for (i in 0..<n) {
            pipeline.ingest(
                FlowFile.create(
                    payload,
                    mutableMapOf("type" to "order", "id" to i.toString())
                )
            )
        }
        val ms = (System.nanoTime() - start) / 1000000

        if (quiet) return
        if (ms > 0) {
            val rate = n.toLong() * 1000L / ms
            System.out.printf("  %,d ff, 2 hops: %dms (%,d ff/s)%n", n, ms, rate)
        } else {
            System.out.printf("  %,d ff, 2 hops: <1ms%n", n)
        }
    }

    private fun printUsage() {
        println("zinc-flow-java — data flow engine")
        println()
        println("Usage:")
        println("  zinc-flow                  Start the worker using ./config.yaml (default)")
        println("  zinc-flow <path>           Start the worker using the given config file")
        println("  zinc-flow validate [path]  Check a config without starting; exit 0 clean, 1 errors, 2 not found")
        println("  zinc-flow bench            Run pipeline throughput benchmarks")
        println("  zinc-flow help             Show this message")
    }
}
