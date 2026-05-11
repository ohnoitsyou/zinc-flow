package zincflow.fabric

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.StreamReadFeature
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.javalin.Javalin
import io.javalin.config.JavalinConfig
import io.javalin.http.Context
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import zincflow.core.FlowFile
import zincflow.fabric.Pipeline.EditResult
import zincflow.fabric.PluginLoader.loadFromDirectory
import zincflow.providers.ProvenanceProvider
import zincflow.providers.ProvenanceProvider.EventType
import zincflow.providers.SchemaRegistryProvider
import zincflow.providers.VersionControlProvider
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.Base64
import java.util.Locale
import java.util.TreeMap
import java.util.concurrent.Executors
import kotlin.collections.mapOf
import kotlin.collections.mutableMapOf
import kotlin.concurrent.Volatile
import kotlin.io.path.absolute
import kotlin.math.max

/** HTTP surface for zinc-flow-java:
 * * GET  /             — dashboard (when the static file is on the classpath)
 * * POST /             — ingest a FlowFile (body = raw payload, X-Flow-*
 * headers become attributes)
 * * GET  /health       — liveness probe
 * * GET  /api/stats    — running counters
 * * GET  /api/processors — list active processors + their type
 * * GET  /api/connections — full connection map
 * * GET  /api/flow     — full graph snapshot (processors + connections + stats)
 * * POST /api/reload   — hot-swap the graph from config.yaml on disk
 * * plus the full admin surface: provenance, processor/source/provider
 * mutation, overlays, plugin management, vc, identity.
 * 
 * HTTP ingest on the management port is a deliberate Java-track choice —
 * enterprise deployments (NiFi shops, etc.) expect inbound HTTP on the
 * worker itself. C# (AOT/edge track) uses a separate ListenHTTP source
 * on its own port instead.
 * 
 * Javalin 6 with Jetty 12 underneath — non-DI, minimal surface. */
class HttpServer @JvmOverloads constructor(
    private val pipeline: Pipeline,
    private val loader: ConfigLoader? = null,
    private val configPath: Path? = null,
    private var plugins: PluginLoader.Summary? = null,
    private val pluginsDir: Path? = null,
    private val identity: NodeIdentity? = null,
    private val json: ObjectMapper = ObjectMapper(YAMLFactory.builder().configure(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION, true).build()).findAndRegisterModules()
) {
//    private val plugins: PluginLoader.Summary
    private var app: Javalin? = null

    @Volatile
    private var boundPort = -1

    fun start(port: Int): HttpServer {
        app = Javalin.create { cfg: JavalinConfig ->
            // Virtual-thread handoff for HTTP requests. Jetty's
            // QueuedThreadPool keeps a small platform-thread pool
            // for accept/select; actual handler work runs on
            // virtual threads, so blocking I/O inside a processor
            // (HTTP calls, disk, DB) no longer ties up an OS
            // thread. Lets a single worker pod fan out to
            // thousands of concurrent ingests. K8s still scales
            // pods horizontally; virtual threads scale within.
            cfg.jetty.threadPool = QueuedThreadPool().apply {
                name = "http-server-qtp"
                virtualThreadsExecutor = Executors.newVirtualThreadPerTaskExecutor()
            }
        }
            // GET / serves the dashboard when the static file is on the classpath.
            // POST / is ingest — a Java-track choice for enterprise HTTP ingress on the worker itself.
            // Distinguishing by method is a zincflow-ism;
            // C# uses a separate ListenHTTP source instead.
            .get("/") { handleDashboard(it) }
            .post("/") { handleIngest(it) }
            .get("/dashboard") { handleDashboard(it) }
            .get("/health") { handleHealth(it) }
            .get("/readyz") { handleReadyz(it) }
            .get("/metrics") { handleMetrics(it) }
            .get("/api/stats") { handleStats(it) }
            .get("/api/processors") { handleProcessors(it) }
            .get("/api/processor-stats") { handleProcessorStats(it) }
            .get("/api/connections") { handleConnections(it) }
            .get("/api/flow") { handleFlow(it) }
            .get("/api/flow/status") { handleFlowStatus(it) }
            .get("/api/registry") { handleRegistry(it) }
            .post("/api/reload") { handleReload(it) }
            .get("/api/providers") { handleProviders(it) }
            .post("/api/providers/enable") { handleEnableProvider(it) }
            .post("/api/providers/disable" ) { handleDisableProvider(it) }
            // Order matters — specific paths register before the
            // {id} wildcard or Javalin binds /api/provenance/failures
            // to handleProvenanceById with id="failures" and 400s.
            .get("/api/provenance") { handleProvenanceRecent(it) }
            .get("/api/provenance/failures") { handleProvenanceFailures(it) }
            .get("/api/provenance/{id}") { handleProvenanceById(it) }
            .post("/api/processors/add") { handleAddProcessor(it) }
            .delete("/api/processors/remove") { handleRemoveProcessor(it) }
            .post("/api/processors/enable") { handleEnableProcessor(it) }
            .post("/api/processors/disable") { handleDisableProcessor(it) }
            .post("/api/processors/state") { handleProcessorState(it) }
            .get("/api/processors/{name}/samples") { handleProcessorSamples(it) }
            .get("/api/layout") { handleGetLayout(it) }
            .get("/api/sources") { handleSources(it) }
            .post("/api/sources/start") { handleStartSource(it) }
            .post("/api/sources/stop") { handleStopSource(it) }
            .get("/api/plugins") { handlePlugins(it) }
            .post("/api/plugins/reload") { handleReloadPlugins(it) }
            .post("/api/connections") { handleAddConnection(it) }
            .delete("/api/connections") { handleRemoveConnection(it) }
            .put("/api/connections/{from}") { handleSetConnections(it) }
            .put("/api/entrypoints") { handleSetEntryPoints(it) }
            .get("/api/overlays") { handleOverlays(it) }
            .get("/api/processor-types") { handleProcessorTypes(it) }
            .get("/api/processor-types/{name}") { handleProcessorType(it) }
            .post("/api/flow/save") { handleFlowSave(it) }
            .get("/api/identity") { handleIdentity(it) }
            .put("/api/processors/{name}/config") { handleUpdateProcessorConfig(it) }
            .put("/api/processors/{name}/connections") { handleSetProcessorConnections(it) }
            .post("/api/processors/{name}/stats/reset") { handleResetProcessorStats(it) }
            .post("/api/flowfiles/ingest") { handleIngestFlowFile(it) }
            .get("/api/vc/status") { handleVcStatus(it) }
            .post("/api/vc/commit") { handleVcCommit(it) }
            .post("/api/vc/push") { handleVcPush(it) }

        // Confluent-shape schema registry — mounted only when a
        // SchemaRegistryProvider is wired into the context. Skipping the
        // mount when absent keeps the route surface honest: tooling that
        // probes /api/schema-registry/subjects gets a clean 404 instead
        // of an endpoint that always returns an error.
        val schemaRegistry = pipeline.context()
            .getProviderAs(SchemaRegistryProvider.NAME, SchemaRegistryProvider::class.java)
        if (schemaRegistry != null && app != null) {
            SchemaRegistryHandler(schemaRegistry).mapRoutes(app!!)
        }

        app!!.start(port)
        boundPort = app!!.port()
        log.info("zinc-flow HTTP server listening on http://localhost:{}", boundPort)
        return this
    }

    fun port(): Int {
        check(boundPort >= 0) { "server not started" }
        return boundPort
    }

    fun stop() {
        app?.stop()
        app = null
        boundPort = -1

        // Release plugin classloaders (both the reload-replaced current
        // one and the startup-supplied one). Safe if either is null.
        currentPlugins?.close()
        currentPlugins = null

        plugins?.close()
    }

    // --- Handlers ---
    private fun handleIngest(ctx: Context) {
        val body = ctx.bodyAsBytes()
        val attributes = mutableMapOf<String, String>()
        ctx.headerMap().forEach { (k: String, v: String) ->
            val lower = k.lowercase(Locale.getDefault())
            if (lower.startsWith("x-flow-")) {
                attributes[lower.substring("x-flow-".length)] = v
            }
        }
        val ff = FlowFile.create(body, attributes)
        try {
            pipeline.ingest(ff)
            ctx.status(202).result(ff.stringId())
        } catch (ex: RuntimeException) {
            log.error("ingest failed for ${ff.stringId()}: $ex", ex)
            ctx.status(500).result("pipeline error: ${ex.message}")
        }
    }


    @JsonDeserialize(using = UIFlowFileDeserializer::class)
    sealed class FlowFileIngest(val target: String, val content: String, val attributes: Map<String, String>) {
        class ContentFlowFile(target: String, content: String, attributes: Map<String, String>) : FlowFileIngest(target, content, attributes)
        class Base64FlowFile(target: String, contentBase64: String, attributes: Map<String, String>): FlowFileIngest(target,
            Base64.getDecoder().decode(contentBase64).contentToString(), attributes)
    }

    class UIFlowFileDeserializer : StdDeserializer<FlowFileIngest>(FlowFileIngest::class.java) {
        companion object {
            private val mapper = jacksonObjectMapper()
        }
        override fun deserialize(
            parser: JsonParser,
            ctx: DeserializationContext,
        ): FlowFileIngest {
            val root = mapper.readTree<ObjectNode>(parser)
            return if (root != null && root.has("contentBase64")) {
                mapper.treeToValue(root, FlowFileIngest.Base64FlowFile::class.java)
            } else if(root != null && root.has("content")) {
                mapper.treeToValue(root, FlowFileIngest.ContentFlowFile::class.java)
            } else {
                throw IllegalArgumentException("Could not determine type")
            }
        }

    }

    /** UI-facing flowfile injection. Accepts JSON
     * {target, content|contentBase64, attributes} and pushes a synthetic
     * FlowFile through the graph. Mirrors the C# `POST /api/flowfiles/ingest`
     * so the shared React UI's test-flowfile dialog works on both tracks. */
    @Throws(Exception::class)
    private fun handleIngestFlowFile(ctx: Context) {
        val body = try {
            json.readValue<FlowFileIngest>(ctx.bodyAsBytes())
        } catch (ex: Exception) {
            writeError(ctx, 400, "body must be a JSON object: " + ex.message)
            return
        }

        val ff = FlowFile.create(body.content.toByteArray(), body.attributes.toMutableMap())

        try {
            if (body.target.isEmpty() || "*" == body.target) {
                pipeline.ingest(ff)
                ctx.contentType("application/json").result(
                    json.writeValueAsBytes(
                        mapOf(
                            "status" to "ingested",
                            "flowfile" to ff.stringId(),
                            "target" to "entry-points"
                        )
                    )
                )
            } else {
                pipeline.ingestAt(ff, body.target)
                ctx.contentType("application/json").result(
                    json.writeValueAsBytes(
                        mapOf(
                            "status" to "ingested",
                            "flowfile" to ff.stringId(),
                            "target" to body.target
                        )
                    )
                )
            }
        } catch (ex: RuntimeException) {
            writeError(ctx, 400, ex.message!!)
        }
    }

    @Throws(Exception::class)
    private fun handleResetProcessorStats(ctx: Context) {
        val name = ctx.pathParam("name")
        if (name.isEmpty()) {
            writeError(ctx, 400, "name required")
            return
        }
        if (!pipeline.graph().processors.containsKey(name)) {
            writeError(ctx, 404, "processor not found")
            return
        }
        pipeline.stats().resetProcessor(name)
        ctx.contentType("application/json").result(
            json.writeValueAsBytes(
                mapOf(
                    "status" to "reset",
                    "name" to name
                )
            )
        )
    }

    @Throws(Exception::class)
    private fun handleStats(ctx: Context) {
        ctx.contentType("application/json").result(json.writeValueAsBytes(summaryStats()))
    }

    /** Dashboard-friendly summary counters returned by `/api/stats`
     * and embedded in `/api/flow`'s `stats` field. Shape
     * matches zinc-flow-csharp's `Fabric.GetStats()`
     * (Fabric.cs:457-463) with camelCase:
     * `{processed, activeExecutions, processors, sources}`.
     * 
     * Detailed counters (per-processor totals, drops, failures) stay
     * on `/api/processor-stats` and the internal
     * [Stats.snapshot] method for programmatic consumers. */
    private fun summaryStats(): Map<String, Any?> {
        return buildMap {
            put("processed", pipeline.stats().snapshot()["totalProcessed"])
            put("activeExecutions", pipeline.metrics().activeExecutions())
            put("processors", pipeline.graph().processors.size)
            put("sources", pipeline.sources().size)
        }
    }

    @Throws(Exception::class)
    private fun handleProcessors(ctx: Context) {
        val out = pipeline.graph().processors.entries
            .associate { (key, _) -> key to pipeline.processorType(key) }
        ctx.contentType("application/json").result(json.writeValueAsBytes(out))
    }

    @Throws(Exception::class)
    private fun handleProcessorStats(ctx: Context) {
        ctx.contentType("application/json").result(json.writeValueAsBytes(pipeline.processorStats()))
    }

    @Throws(Exception::class)
    private fun handleConnections(ctx: Context) {
        ctx.contentType("application/json").result(json.writeValueAsBytes(pipeline.graph().connections))
    }

    @Throws(Exception::class)
    private fun handleFlow(ctx: Context) {
        val graph = pipeline.graph()
        val processors = mutableListOf<Map<String, Any>>()
        val perProcStats = pipeline.processorStats()
        for (entry in graph.processors.entries) {
            val name = entry.key
            val info = buildMap {
                put("name", name)
                put(ConfigLoader.TYPE_KEY, pipeline.processorType(name))
                put("state", pipeline.processorState(name).name)
                put("config", pipeline.processorConfig(name))
                put("stats", perProcStats.getOrDefault(name, emptyMap()))
                put("connections", graph.connections.getOrDefault(name, emptyMap()))
            }
            processors.add(info)
        }

        val context = pipeline.context()
        val providers = context.listProviders().map { providerName ->
            val p = context.getProvider(providerName)
            mapOf(
                "name" to providerName,
                "type" to (p?.providerType() ?: "unknown"),
                "state" to (p?.state()?.name ?: "UNKNOWN")
            )
        }

        val sources: List<Map<String, Any>> = pipeline.sources().values.map { source ->
            mapOf("name" to source.name(), "type" to source.sourceType(), "running" to  source.isRunning)
        }

        val out = buildMap {
            put("entryPoints", graph.entryPoints)
            put("processors", processors)
            // Top-level `connections` is a Java-track add-on (the current UI's
            // FlowController + BfsLayout read from here). C# doesn't expose
            // it; the same data is inline on each processor. Tracked as a
            // C#-cohort candidate for consistency.
            put("connections", graph.connections)
            put("providers", providers)
            put("sources", sources)
            // Embedded stats match /api/stats summary shape (processed,
            // activeExecutions, processors, sources) — not the detailed
            // Stats.snapshot(). Dashboards only need the summary; callers
            // that want totals/errors/drops go to /api/processor-stats.
            put("stats", summaryStats())
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(out))
    }

    private fun handleFlowStatus(ctx: Context) {
        val dirtyStatus = PipelineMetrics.getDirtyStatus()
        val body = buildMap {
            put("dirty", dirtyStatus.isClean)
            put("mutationCounter", dirtyStatus.mutationCounter)
            put("lastSavedCounter", dirtyStatus.saveCounter)
            put("lastSaveTick", dirtyStatus.saveTick)
        }

        ctx.contentType("application/json").result(json.writeValueAsBytes(body))
    }

    @Throws(Exception::class)
    private fun handleRegistry(ctx: Context) {
        // Emit the latest version of each type, matching the C# worker's
        // shape so the shared React UI consumes both uniformly.
        val latestByName = TreeMap<String, Registry.TypeInfo>()
        for (info in pipeline.registry().listAll()) {
            val prev = latestByName[info.name]
            if (prev == null || TypeRefs.compareVersions(info.version, prev.version) > 0) {
                latestByName[info.name] = info
            }
        }

        val out = latestByName.values.map { typeInfoToJson(it) }
        ctx.contentType("application/json").result(json.writeValueAsBytes(out))
    }

    private fun handleMetrics(ctx: Context) {
        ctx.contentType("text/plain; version=0.0.4; charset=utf-8")
            .result(pipeline.metrics().scrape())
    }

    private fun handleDashboard(ctx: Context) {
        try {
            HttpServer::class.java.classLoader.getResourceAsStream("dashboard.html").use { inStream ->
                if (inStream == null) {
                    ctx.status(404).result("dashboard.html not on classpath")
                    return
                }
                ctx.contentType("text/html; charset=utf-8").result(inStream.readAllBytes())
            }
        } catch (ex: IOException) {
            ctx.status(500).result("dashboard read failed: " + ex.message)
        }
    }

    @Throws(Exception::class)
    private fun handleReload(ctx: Context) {
        if (loader == null || configPath == null) {
            writeError(ctx, 501, "reload not supported: server started without a config loader")
            return
        }
        try {
            val fresh = loader.loadFromFile(configPath)
            val diff = pipeline.applyReload(fresh)
            log.info(
                "reloaded pipeline from {} — {} processors, entry points: {}, diff: {}",
                configPath, fresh.processors.size, fresh.entryPoints, diff
            )
            ctx.json(mapOf<String, Any>(
                    "status" to "reloaded",
                    "added" to diff.added,
                    "removed" to diff.removed,
                    "updated" to diff.updated,
                    "connectionsChanged" to diff.connectionsChanged,
                    "total" to diff.total()
                )
            )
        } catch (ex: IOException) {
            log.error("reload failed: {}", ex.toString(), ex)
            writeError(ctx, 400, "reload failed: " + ex.message)
        } catch (ex: RuntimeException) {
            log.error("reload failed: {}", ex.toString(), ex)
            writeError(ctx, 400, "reload failed: " + ex.message)
        }
    }

    // --- Health ---
    @Throws(Exception::class)
    private fun handleHealth(ctx: Context) {
        val sources: List<Map<String, Any>> = pipeline.sources().values.map { source ->
            mapOf("name" to source.name(), "type" to source.sourceType(), "running" to source.isRunning)
        }
        ctx.contentType("application/json").result(
            json.writeValueAsBytes(
                mapOf(
                    "status" to "healthy",
                    "sources" to sources
                )
            )
        )
    }

    // --- Readyz ---
    @Throws(Exception::class)
    private fun handleReadyz(ctx: Context) {
        val processors = pipeline.graph().processors
        val sources = pipeline.sources().entries
        val notRunning = sources.filterNot { source -> source.value.isRunning }
        val ready = processors.count() > 0 && notRunning.count() == 0

        val body = buildMap {
            put("ready", ready)
            put("processors", processors.count())
            put("sourcesTotal", sources.count())
            put("sourcesNotRunning", notRunning)
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(body))
    }

    // --- Providers ---
    @Throws(Exception::class)
    private fun handleProviders(ctx: Context) {
        val context = pipeline.context()
        val out = context.listProviders().map { providerName ->
            val p = context.getProvider(providerName)
            mapOf(
                "name" to providerName,
                "type" to (p?.providerType() ?: "unknown"),
                "state" to (p?.state()?.name ?: "UNKNOWN")
            )
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(out))
    }

    @Throws(Exception::class)
    private fun handleEnableProvider(ctx: Context) {
        val name = nameFromBody(ctx)
        if (name.isEmpty()) {
            writeError(ctx, 400, "name required")
            return
        }
        val ok = pipeline.enableProvider(name)
        writeStatus(ctx, ok, name, "enabled", "provider not found")
    }

    @Throws(Exception::class)
    private fun handleDisableProvider(ctx: Context) {
        val name = nameFromBody(ctx)
        if (name.isEmpty()) {
            writeError(ctx, 400, "name required")
            return
        }
        val ok = pipeline.disableProvider(name)
        writeStatus(ctx, ok, name, "disabled", "provider not found")
    }

    // --- Provenance ---
    @Throws(Exception::class)
    private fun handleProvenanceRecent(ctx: Context) {
        val prov = pipeline.provenance()
        if (prov == null) {
            writeError(ctx, 503, "provenance provider not enabled")
            return
        }
        var n = 50
        val nStr = ctx.queryParam("n")
        if (!nStr.isNullOrEmpty()) {
            try {
                n = nStr.toInt()
            } catch (_: NumberFormatException) {
                writeError(ctx, 400, "query param 'n' is not an integer: '$nStr'")
                return
            }
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(shape(prov.getRecent(n))))
    }

    @Throws(Exception::class)
    private fun handleProvenanceById(ctx: Context) {
        val prov = pipeline.provenance()
        if (prov == null) {
            writeError(ctx, 503, "provenance provider not enabled")
            return
        }
        val id: Long
        try {
            id = ctx.pathParam("id").toLong()
        } catch (_: NumberFormatException) {
            writeError(ctx, 400, "path param 'id' is not a long")
            return
        }
        val events = prov.getEvents(id)
        val shape = shape(events)
        ctx.contentType("application/json").result(json.writeValueAsBytes(shape))
    }

    // --- Processor admin ---
    @Throws(Exception::class)
    private fun handleAddProcessor(ctx: Context) {
        val body = readJsonBody(ctx)
        if (body == null) {
            writeError(ctx, 400, "invalid json body")
            return
        }
        try {
            val name = body["name"]?.toString().takeIf { it?.isNotEmpty() == true } ?: throw IllegalArgumentException("Name is required but not provided")
            val type = body[ConfigLoader.TYPE_KEY]?.toString().takeIf { it?.isNotEmpty() == true } ?: throw IllegalArgumentException("Type is required but not provided")

//        val name: String = str(body["name"])
//            val type: String = str(body[ConfigLoader.TYPE_KEY])
//            if (name.isEmpty() || type.isEmpty()) {
//                writeError(ctx, 400, "name and type required")
//                return
//            }

            val config: Map<String, String> = asStringMap(body[ConfigLoader.CONFIG_KEY])
            val requires: List<String> = asStringList(body["requires"])
            val connections: Map<String, List<String>> = asConnections(body["connections"])

            val ok = pipeline.addProcessor(name, type, config, requires, connections)
            writeStatus(ctx, ok, name, "created", "processor already exists or unknown type")
        } catch (e: IllegalArgumentException) {
            writeError(ctx, 400, e.message ?: "Exception adding processor")
        }
    }

    @Throws(Exception::class)
    private fun handleRemoveProcessor(ctx: Context) {
        val name = nameFromBody(ctx)
        if (name.isEmpty()) {
            writeError(ctx, 400, "name required")
            return
        }
        val ok = pipeline.removeProcessor(name)
        writeStatus(ctx, ok, name, "removed", "processor not found")
    }

    @Throws(Exception::class)
    private fun handleEnableProcessor(ctx: Context) {
        val name = nameFromBody(ctx)
        if (name.isEmpty()) {
            writeError(ctx, 400, "name required")
            return
        }
        val ok = pipeline.enableProcessor(name)
        writeStatus(ctx, ok, name, "enabled", "processor not found")
    }

    @Throws(Exception::class)
    private fun handleDisableProcessor(ctx: Context) {
        val name = nameFromBody(ctx)
        if (name.isEmpty()) {
            writeError(ctx, 400, "name required")
            return
        }
        val ok = pipeline.disableProcessor(name)
        writeStatus(ctx, ok, name, "disabled", "processor not found")
    }

    @Throws(Exception::class)
    private fun handleProcessorState(ctx: Context) {
        val name = nameFromBody(ctx)
        if (name.isEmpty()) {
            writeError(ctx, 400, "name required")
            return
        }
        if (!pipeline.graph().processors.containsKey(name)) {
            writeError(ctx, 404, "processor not found")
            return
        }
        ctx.contentType("application/json").result(
            json.writeValueAsBytes(
                mapOf("name" to name, "state" to pipeline.processorState(name).name)
            )
        )
    }

    @Throws(Exception::class)
    private fun handleProcessorSamples(ctx: Context) {
        val name = ctx.pathParam("name")
        if(name.isEmpty()) {
            writeError(ctx, 400, "Name required")
        }
        val snapshot = SampleRegistry.getSnapshotFor(name)
        val samples = snapshot.map {
            mapOf(
                "timestamp" to it.timestamp,
                "flowfile" to "ff-${it.flowfileId}",
                "contentType" to it.contentType,
                "preview" to SampleRegistry.previewAsString(it.preview),
                "previewBase64" to if(it.contentType == "records") null else kotlin.io.encoding.Base64.encode(it.preview),
                "attributes" to it.attributes
            )
        }
        ctx.contentType("application/json")
            .result(json.writeValueAsString(mapOf(
                "name" to name,
                "sampling" to SampleRegistry.samplingEnabled,
                "samples" to samples
            )
        ))
    }

    @Throws(Exception::class)
    private fun handleGetLayout(ctx: Context) {
        val path = configPath?.absolute()?.resolveSibling("layout.yaml")?.toString() ?: ""
        if (path.isEmpty() || !File(path).exists()) {
            ctx.json(mapOf("positions" to emptyMap<String, String>()))
        } else  {
            val positions = json.readValue(File(path), Layout::class.java)
            ctx.json(mapOf("positions" to positions.positions, "path" to path))
        }
    }

    data class Layout(val positions: Map<String, LayoutXY>)
    data class LayoutXY(val x: Double, val y: Double)

    // --- Sources ---
    @Throws(Exception::class)
    private fun handleSources(ctx: Context) {
        val out = pipeline.sources().values.map { s ->
            mapOf<String, Any>("name" to s.name(), "type" to s.sourceType(), "running" to s.isRunning)
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(out))
    }

    @Throws(Exception::class)
    private fun handleStartSource(ctx: Context) {
        val name = nameFromBody(ctx)
        if (name.isEmpty()) {
            writeError(ctx, 400, "name required")
            return
        }
        val ok = pipeline.startSource(name)
        writeStatus(ctx, ok, name, "started", "source not found")
    }

    @Throws(Exception::class)
    private fun handleStopSource(ctx: Context) {
        val name = nameFromBody(ctx)
        if (name.isEmpty()) {
            writeError(ctx, 400, "name required")
            return
        }
        val ok = pipeline.stopSource(name)
        writeStatus(ctx, ok, name, "stopped", "source not found")
    }

    // --- Graph mutation (connections + entry points) ---
    // TODO: Parse using data class
    @Throws(Exception::class)
    private fun handleAddConnection(ctx: Context) {
        val body = readJsonBody(ctx)
        if (body == null) {
            writeError(ctx, 400, "invalid json body")
            return
        }
        val from: String = str(body["from"])
        val rel: String = str(body["relationship"])
        val to: String = str(body["to"])
        writeEditResult(
            ctx, pipeline.addConnection(from, rel, to),
            mapOf("status" to "added", "from" to from, "relationship" to rel, "to" to to)
        )
    }

    // TODO: Parse using data class
    @Throws(Exception::class)
    private fun handleRemoveConnection(ctx: Context) {
        val body = readJsonBody(ctx)
        if (body == null) {
            writeError(ctx, 400, "invalid json body")
            return
        }
        val from: String = str(body["from"])
        val rel: String = str(body["relationship"])
        val to: String = str(body["to"])
        writeEditResult(
            ctx, pipeline.removeConnection(from, rel, to),
            mapOf("status" to "removed", "from" to from, "relationship" to rel, "to" to to)
        )
    }

    // TODO: Parse using data class
    @Throws(Exception::class)
    private fun handleSetConnections(ctx: Context) {
        val from = ctx.pathParam("from")
        val body = readJsonBody(ctx)
        if (body == null) {
            writeError(ctx, 400, "invalid json body")
            return
        }
        val relationships = mutableMapOf<String, List<String>>()
        for (entry in body.entries) {
            val list = entry.value
            if (list is List<*>) {
                val targets = list.map { str(it) }
                relationships[entry.key] = targets
            } else {
                writeError(ctx, 400, "relationship '" + entry.key + "' must map to a list of target names")
                return
            }
        }
        writeEditResult(
            ctx, pipeline.setConnections(from, relationships),
            mapOf("status" to "replaced", "from" to from, "relationships" to relationships)
        )
    }

    // TODO: Parse using data class
    @Throws(Exception::class)
    private fun handleSetEntryPoints(ctx: Context) {
        val body = readJsonBody(ctx)
        if (body == null) {
            writeError(ctx, 400, "invalid json body")
            return
        }
        val namesObj = body["names"]
        if (namesObj !is List<*>) {
            writeError(ctx, 400, "body must include a 'names' array")
            return
        }
        val names = namesObj.map { str(it) }
        writeEditResult(
            ctx, pipeline.setEntryPoints(names),
            mapOf("status" to "replaced", "names" to names)
        )
    }

    @Throws(Exception::class)
    private fun writeEditResult(ctx: Context, result: EditResult, okBody: Map<String, Any>) {
        if (result.ok) {
            ctx.status(200).contentType("application/json").result(json.writeValueAsBytes(okBody))
        } else {
            // 409 when the state is inconsistent (duplicate edge, missing
            // target, unknown type), 400 for blank-input kinds of errors.
            // Pipeline.EditResult only distinguishes a single 'reason'
            // string so we route by content.
            val reason = result.reason
            val status = if (reason.contains("not found")
                || reason.contains("already exists")
                || reason.contains("unknown")
            ) 409 else 400
            writeError(ctx, status, reason)
        }
    }

    // --- Version control (opt-in git provider) ---
    private fun vc(): VersionControlProvider? {
        return pipeline.context()
            .getProviderAs(VersionControlProvider.NAME, VersionControlProvider::class.java)
    }

    @Throws(Exception::class)
    private fun handleVcStatus(ctx: Context) {
        val v = vc()
        if (v == null || !v.isEnabled) {
            ctx.contentType("application/json").result(
                json.writeValueAsBytes(
                    mapOf("enabled" to false)
                )
            )
            return
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(v.statusJson()))
    }

    @Throws(Exception::class)
    private fun handleVcCommit(ctx: Context) {
        val v = vc()
        if (v == null || !v.isEnabled) {
            writeError(ctx, 503, "version control provider not enabled")
            return
        }
        val body = readJsonBody(ctx)
        if (body == null) {
            writeError(ctx, 400, "invalid json body")
            return
        }
        val message = str(body["message"])
        if (message.isEmpty()) {
            writeError(ctx, 400, "commit message must not be blank")
            return
        }
        val path = if (body["path"] == null) null else str(body["path"])

        val r = v.commit(path, message)
        val out = buildMap<String, Any> {
            put("ok", r.ok)
            put("exitCode", r.exitCode)
            put("stdout", r.stdout)
            put("stderr", r.stderr)
        }
        ctx.status(if (r.ok) 200 else 500).contentType("application/json")
            .result(json.writeValueAsBytes(out))
    }

    @Throws(Exception::class)
    private fun handleVcPush(ctx: Context) {
        val v = vc()
        if (v == null || !v.isEnabled) {
            writeError(ctx, 503, "version control provider not enabled")
            return
        }
        val r = v.push()
        val out = buildMap<String, Any> {
            put("ok", r.ok)
            put("exitCode", r.exitCode)
            put("stdout", r.stdout)
            put("stderr", r.stderr)
        }
        ctx.status(if (r.ok) 200 else 500).contentType("application/json")
            .result(json.writeValueAsBytes(out))
    }

    // --- Provenance failures ---
    @Throws(Exception::class)
    private fun handleProvenanceFailures(ctx: Context) {
        val prov = pipeline.provenance()
        if (prov == null) {
            writeError(ctx, 503, "provenance provider not enabled")
            return
        }
        var n = 50
        val nStr = ctx.queryParam("n")
        if (!nStr.isNullOrEmpty()) {
            try {
                n = nStr.toInt()
            } catch (_: NumberFormatException) {
                writeError(ctx, 400, "query param 'n' is not an integer: '$nStr'")
                return
            }
        }
        // Over-fetch to give the filter a real chance of returning n
        // failures even when non-failure events dominate the ring buffer.
        val window = max(n * 20, 500)
        val recent = prov.getRecent(window)
        val failures = recent.asSequence().filter { it.type == EventType.FAILED }.take(n).toList()
        ctx.contentType("application/json").result(json.writeValueAsBytes(shape(failures)))
    }

    // --- Processor config / connection updates ---
    @Throws(Exception::class)
    private fun handleUpdateProcessorConfig(ctx: Context) {
        val name = ctx.pathParam("name")
        val body = readJsonBody(ctx)
        if (body == null) {
            writeError(ctx, 400, "invalid json body")
            return
        }
        val type: String? = if (body[ConfigLoader.TYPE_KEY] == null) null else str(body[ConfigLoader.TYPE_KEY])
        val config = asStringMap(body[ConfigLoader.CONFIG_KEY])

        val r = pipeline.updateProcessorConfig(name, type, config)
        writeEditResult(ctx, r, mapOf("status" to "updated", "name" to name))
    }

    // TODO: Process using data class
    @Throws(Exception::class)
    private fun handleSetProcessorConnections(ctx: Context) {
        val name = ctx.pathParam("name")
        val body = readJsonBody(ctx)
        if (body == null) {
            writeError(ctx, 400, "invalid json body")
            return
        }
        val relationships = mutableMapOf<String, List<String>>()
        for (entry in body.entries) {
            if (entry.value is List<*>) {
                val list = entry.value as List<String>
                val targets = list.map { v -> str(v) }
                relationships[entry.key] = targets
            } else {
                writeError(ctx, 400, "relationship '" + entry.key + "' must map to a list of target names")
                return
            }
        }
        val r = pipeline.setConnections(name, relationships)
        writeEditResult(ctx, r, mapOf("status" to "replaced", "from" to name, "relationships" to relationships))
    }

    // --- Identity ---
    @Throws(Exception::class)
    private fun handleIdentity(ctx: Context) {
        if (identity == null) {
            writeError(ctx, 503, "identity not wired — server started without a NodeIdentity")
            return
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(identity.toMap(port())))
    }

    // --- Flow save ---
    @Throws(Exception::class)
    private fun handleFlowSave(ctx: Context) {
        if (loader == null || configPath == null) {
            writeError(ctx, 501, "flow save unavailable — server started without a config path")
            return
        }
        val yaml = YamlEmitter.emit(pipeline.graph(), loader.lastSpecs())
        try {
            if (yaml != null) {
                Files.writeString(configPath, yaml)
            }
        } catch (ex: IOException) {
            writeError(ctx, 500, "flow save failed: " + ex.message)
            return
        }
        log.info("flow saved to {}", configPath)

        val body= buildMap {
            put("status", "saved")
            put("path", configPath.toString())
            put("bytes", yaml?.length)
            put("committed", false)
            put("pushed", false)
        }.toMutableMap()

        // VC-aware save: if VersionControlProvider is enabled, also
        // stage+commit the config file and optionally push to the
        // configured remote. One UI button → one backend call → right
        // thing happens. Matches zinc-flow-csharp's POST /api/flow/save
        // behavior so the UI sees the same response shape on both tracks.
        val vc = pipeline.context().getProviderAs("version_control", VersionControlProvider::class.java)
        if (vc?.isEnabled == true) {
            val reqBody = readJsonBody(ctx)
            var message = "flow: update via UI"
            var push = true
            if (reqBody != null) {
                val m = reqBody["message"]
                if (m is String && !m.isBlank()) message = m
                val p = reqBody["push"]
                if (p is Boolean) push = p
            }
            val relPath = configPath.fileName.toString()
            val commitRes = vc.commit(relPath, message)
            body["committed"] = commitRes.ok
            body["commitExitCode"] = commitRes.exitCode
            body["commitStdout"] = commitRes.stdout
            if (!commitRes.ok) body["commitStderr"] = commitRes.stderr

            if (commitRes.ok && push) {
                val pushRes = vc.push()
                body["pushed"] = pushRes.ok
                body["pushExitCode"] = pushRes.exitCode
                body["pushStdout"] = pushRes.stdout
                if (!pushRes.ok) body["pushStderr"] = pushRes.stderr
            }
        }

        PipelineMetrics.recordSave()
        ctx.status(200).contentType("application/json").result(json.writeValueAsBytes(body))
    }

    // --- Processor types (versioned registry) ---
    @Throws(Exception::class)
    private fun handleProcessorTypes(ctx: Context) {
        val out = pipeline.registry().listAll().map {
            typeInfoToJson(it)
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(out))
    }

    @Throws(Exception::class)
    private fun handleProcessorType(ctx: Context) {
        val r = pipeline.registry()
        val name = ctx.pathParam("name")
        val versions = r.listVersions(name)
        if (versions.isEmpty()) {
            writeError(ctx, 404, "processor type '$name' not found")
            return
        }
        val body = buildMap {
            put("name", name)
            put("latest", if (r.latest(name) == null) null else r.latest(name)?.version)
            put("versions", versions.map { typeInfoToJson(it) })
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(body))
    }

    // --- Overlays ---
    // TODO: Change to use data class
    @Throws(Exception::class)
    private fun handleOverlays(ctx: Context) {
        val resolved = loader?.lastOverlay()
        if (resolved == null) {
            writeError(ctx, 503, "overlay info unavailable — server started without a config loader")
            return
        }
        val layers = mutableListOf<MutableMap<String, Any?>>()
        for (layer in resolved.layers) {
            val info = mutableMapOf<String, Any?>()
            info["role"] = layer.role
            info["path"] = layer.path?.toString()
            info["present"] = layer.present
            info["size"] = layer.content.size
            layers.add(info)
        }
        val body = mutableMapOf<String, Any?>()
        body["base"] = resolved.basePath?.toString()
        body["layers"] = layers
        body["effective"] = resolved.effective
        body["provenance"] = resolved.provenance
        ctx.contentType("application/json").result(json.writeValueAsBytes(body))
    }

    // --- Plugins ---
    @Volatile
    private var currentPlugins: PluginLoader.Summary? = null

    /** Full constructor — wires in the plugin summary and node identity
     * so `/api/plugins` and `/api/identity` can answer. */
    /** Constructor for the config-driven path — enables `/api/reload`. */
    init {
        this.plugins = plugins ?: PluginLoader.Summary.empty()
    }

    private fun pluginSummary(): PluginLoader.Summary {
        return (if (currentPlugins != null) currentPlugins else plugins) ?: PluginLoader.Summary.empty()
    }

    @Throws(Exception::class)
    private fun handlePlugins(ctx: Context) {
        ctx.contentType("application/json")
            .result(json.writeValueAsBytes(PluginLoader.toJson(pluginSummary())))
    }

    /** Re-scan the plugins directory and register any new jars that
     * appeared since startup. Existing entries are overwritten (last
     * loader wins) — convenient for dev, relies on the operator to
     * avoid removing a processor type that's still referenced in the
     * running flow. */
    @Throws(Exception::class)
    private fun handleReloadPlugins(ctx: Context) {
        if (pluginsDir == null) {
            writeError(ctx, 501, "plugins directory was not configured at startup")
            return
        }
        // Release the previous URLClassLoader before replacing it — if
        // we leak these, repeated reloads pin every plugin jar open
        // and exhaust file handles over time.
        val prior = currentPlugins
        currentPlugins = loadFromDirectory(pluginsDir, pipeline.registry(), pipeline.context())
        prior?.close()
        log.info("reloaded plugins from {} — {} loaded", pluginsDir, currentPlugins!!.totalLoaded())
        ctx.contentType("application/json")
            .result(json.writeValueAsBytes(PluginLoader.toJson(currentPlugins!!)))
    }

    // --- Body + response helpers ---
    private fun readJsonBody(ctx: Context): Map<String, Any>? {
        return try {
            json.readValue(ctx.bodyAsBytes(), Map::class.java) as? Map<String, Any>
        } catch (_: Exception) {
            null
        }
    }

    private fun nameFromBody(ctx: Context): String {
        val body = readJsonBody(ctx)
        return if (body == null) "" else str(body["name"])
    }

    @Throws(Exception::class)
    private fun writeStatus(ctx: Context, ok: Boolean, name: String, okStatus: String, failMessage: String) {
        val body = if (ok) {
            mapOf("status" to okStatus, "name" to name)
        } else {
            mapOf("error" to failMessage, "name" to name)
        }
        ctx.status(if (ok) 200 else 404)
            .contentType("application/json")
            .result(json.writeValueAsBytes(body))
    }

    @Throws(Exception::class)
    private fun writeError(ctx: Context, status: Int, message: String) {
        ctx.status(status)
            .contentType("application/json")
            .result(json.writeValueAsBytes(mapOf("error" to message)))
    }

    // Exposed for tests/inspection.
    fun routes(): MutableList<String> {
        return mutableListOf(
            "GET /", "POST /", "GET /dashboard", "GET /health", "GET /metrics",
            "GET /api/stats", "GET /api/processors", "GET /api/processor-stats",
            "GET /api/connections", "GET /api/flow", "GET /api/registry",
            "POST /api/reload",
            "GET /api/providers", "POST /api/providers/enable", "POST /api/providers/disable",
            "GET /api/provenance", "GET /api/provenance/{id}",
            "POST /api/processors/add", "DELETE /api/processors/remove",
            "POST /api/processors/enable", "POST /api/processors/disable",
            "POST /api/processors/state",
            "GET /api/sources", "POST /api/sources/start", "POST /api/sources/stop",
            "GET /api/plugins", "POST /api/plugins/reload",
            "POST /api/connections", "DELETE /api/connections",
            "PUT /api/connections/{from}", "PUT /api/entrypoints",
            "GET /api/overlays",
            "GET /api/processor-types", "GET /api/processor-types/{name}",
            "POST /api/flow/save", "GET /api/identity",
            "PUT /api/processors/{name}/config",
            "PUT /api/processors/{name}/connections",
            "GET /api/provenance/failures",
            "GET /api/vc/status", "POST /api/vc/commit", "POST /api/vc/push"
        )
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(HttpServer::class.java)

        private fun shape(events: List<ProvenanceProvider.Event>): List<Map<String, Any>> {
            return events.map { e -> mapOf<String, Any>(
                    "flowFileId" to e.flowFileId,
                    "type" to e.type!!.name,
                    "component" to (e.component ?: ""),
                    "details" to (e.details ?: ""),
                    "timestampMillis" to e.timestampMillis
                )
            }
        }

        private fun typeInfoToJson(info: Registry.TypeInfo): Map<String, Any> {
            return buildMap {
                put("name", info.name)
                put("version", info.version)
                put("description", info.description ?: "")
                put("category", info.category ?: "Other")
                put("configKeys", info.configKeys)
                put("relationships", info.relationships)
                put("parameters", info.parameters.map { it.asMap() })
            }
        }
        private fun str(o: Any?): String {
            return o?.toString() ?: ""
        }

        private fun asStringMap(raw: Any?): Map<String, String> {
            return if (raw is Map<*, *>) {
                raw.entries.associate { (key, value) -> key.toString() to str(value) }
            } else {
                mapOf()
            }
        }

        private fun asStringList(raw: Any?): List<String> {
            if (raw !is List<*>) return listOf()
            return raw.map { str(it) }
        }

        private fun asConnections(raw: Any?): Map<String, List<String>> {
            val map = raw as? Map<*, *> ?: return mapOf()
            return map.entries.associateTo(mutableMapOf()) { (key, value) ->
                val relationship = key.toString()
                val targets = when(value) {
                    is List<*> -> value.map { it.toString() }
                    null -> emptyList()
                    else -> listOf(value.toString())
                }
                relationship to targets
            }
        }
    }
}
