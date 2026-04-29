package zincflow.fabric

import com.fasterxml.jackson.databind.ObjectMapper
import io.javalin.Javalin
import io.javalin.config.JavalinConfig
import io.javalin.http.Context
import io.javalin.http.Handler
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import zincflow.core.FlowFile
import zincflow.fabric.Pipeline.EditResult
import zincflow.providers.ProvenanceProvider
import zincflow.providers.SchemaRegistryProvider
import zincflow.providers.VersionControlProvider
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.Base64
import java.util.Locale
import java.util.Map
import java.util.TreeMap
import java.util.concurrent.Executors
import java.util.function.Consumer
import kotlin.collections.mutableMapOf
import kotlin.concurrent.Volatile
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
    private val pipeline: Pipeline, private val loader: ConfigLoader? = null, private val configPath: Path? = null,
    plugins: PluginLoader.Summary? = null, private val pluginsDir: Path? = null,
    private val identity: NodeIdentity? = null
) {
    private val plugins: PluginLoader.Summary
    private val json = ObjectMapper()
    private var app: Javalin? = null

    @Volatile
    private var boundPort = -1

    fun start(port: Int): HttpServer {
        app = Javalin.create(Consumer { cfg: JavalinConfig? ->
            // Virtual-thread handoff for HTTP requests. Jetty's
            // QueuedThreadPool keeps a small platform-thread pool
            // for accept/select; actual handler work runs on
            // virtual threads, so blocking I/O inside a processor
            // (HTTP calls, disk, DB) no longer ties up an OS
            // thread. Lets a single worker pod fan out to
            // thousands of concurrent ingests. K8s still scales
            // pods horizontally; virtual threads scale within.
            val qtp = QueuedThreadPool()
            qtp.setName("zinc-flow-jetty")
            qtp.setVirtualThreadsExecutor(Executors.newVirtualThreadPerTaskExecutor())
            cfg!!.jetty.threadPool = qtp
        }) // GET / serves the dashboard when the static file is on the
            // classpath. POST / is ingest — a Java-track choice for
            // enterprise HTTP ingress on the worker itself. Distinguishing
            // by method is a zincflow-ism; C# uses a separate
            // ListenHTTP source instead.
            .get("/", Handler { ctx: Context? -> this.handleDashboard(ctx!!) })
            .post("/", Handler { ctx: Context? -> this.handleIngest(ctx!!) })
            .get("/dashboard", Handler { ctx: Context? -> this.handleDashboard(ctx!!) })
            .get("/health", Handler { ctx: Context? -> this.handleHealth(ctx!!) })
            .get("/metrics", Handler { ctx: Context? -> this.handleMetrics(ctx!!) })
            .get("/api/stats", Handler { ctx: Context? -> this.handleStats(ctx!!) })
            .get("/api/processors", Handler { ctx: Context? -> this.handleProcessors(ctx!!) })
            .get("/api/processor-stats", Handler { ctx: Context? -> this.handleProcessorStats(ctx!!) })
            .get("/api/connections", Handler { ctx: Context? -> this.handleConnections(ctx!!) })
            .get("/api/flow", Handler { ctx: Context? -> this.handleFlow(ctx!!) })
            .get("/api/registry", Handler { ctx: Context? -> this.handleRegistry(ctx!!) })
            .post("/api/reload", Handler { ctx: Context? -> this.handleReload(ctx!!) })
            .get("/api/providers", Handler { ctx: Context? -> this.handleProviders(ctx!!) })
            .post("/api/providers/enable", Handler { ctx: Context? -> this.handleEnableProvider(ctx!!) })
            .post(
                "/api/providers/disable",
                Handler { ctx: Context? -> this.handleDisableProvider(ctx!!) }) // Order matters — specific paths register before the
            // {id} wildcard or Javalin binds /api/provenance/failures
            // to handleProvenanceById with id="failures" and 400s.
            .get("/api/provenance", Handler { ctx: Context? -> this.handleProvenanceRecent(ctx!!) })
            .get("/api/provenance/failures", Handler { ctx: Context? -> this.handleProvenanceFailures(ctx!!) })
            .get("/api/provenance/{id}", Handler { ctx: Context? -> this.handleProvenanceById(ctx!!) })
            .post("/api/processors/add", Handler { ctx: Context? -> this.handleAddProcessor(ctx!!) })
            .delete("/api/processors/remove", Handler { ctx: Context? -> this.handleRemoveProcessor(ctx!!) })
            .post("/api/processors/enable", Handler { ctx: Context? -> this.handleEnableProcessor(ctx!!) })
            .post("/api/processors/disable", Handler { ctx: Context? -> this.handleDisableProcessor(ctx!!) })
            .post("/api/processors/state", Handler { ctx: Context? -> this.handleProcessorState(ctx!!) })
            .get("/api/sources", Handler { ctx: Context? -> this.handleSources(ctx!!) })
            .post("/api/sources/start", Handler { ctx: Context? -> this.handleStartSource(ctx!!) })
            .post("/api/sources/stop", Handler { ctx: Context? -> this.handleStopSource(ctx!!) })
            .get("/api/plugins", Handler { ctx: Context? -> this.handlePlugins(ctx!!) })
            .post("/api/plugins/reload", Handler { ctx: Context? -> this.handleReloadPlugins(ctx!!) })
            .post("/api/connections", Handler { ctx: Context? -> this.handleAddConnection(ctx!!) })
            .delete("/api/connections", Handler { ctx: Context? -> this.handleRemoveConnection(ctx!!) })
            .put("/api/connections/{from}", Handler { ctx: Context? -> this.handleSetConnections(ctx!!) })
            .put("/api/entrypoints", Handler { ctx: Context? -> this.handleSetEntryPoints(ctx!!) })
            .get("/api/overlays", Handler { ctx: Context? -> this.handleOverlays(ctx!!) })
            .get("/api/processor-types", Handler { ctx: Context? -> this.handleProcessorTypes(ctx!!) })
            .get("/api/processor-types/{name}", Handler { ctx: Context? -> this.handleProcessorType(ctx!!) })
            .post("/api/flow/save", Handler { ctx: Context? -> this.handleFlowSave(ctx!!) })
            .get("/api/identity", Handler { ctx: Context? -> this.handleIdentity(ctx!!) })
            .put("/api/processors/{name}/config", Handler { ctx: Context? -> this.handleUpdateProcessorConfig(ctx!!) })
            .put(
                "/api/processors/{name}/connections",
                Handler { ctx: Context? -> this.handleSetProcessorConnections(ctx!!) })
            .post(
                "/api/processors/{name}/stats/reset",
                Handler { ctx: Context? -> this.handleResetProcessorStats(ctx!!) })
            .post("/api/flowfiles/ingest", Handler { ctx: Context? -> this.handleIngestFlowFile(ctx!!) })
            .get("/api/vc/status", Handler { ctx: Context? -> this.handleVcStatus(ctx!!) })
            .post("/api/vc/commit", Handler { ctx: Context? -> this.handleVcCommit(ctx!!) })
            .post("/api/vc/push", Handler { ctx: Context? -> this.handleVcPush(ctx!!) })

        // Confluent-shape schema registry — mounted only when a
        // SchemaRegistryProvider is wired into the context. Skipping the
        // mount when absent keeps the route surface honest: tooling that
        // probes /api/schema-registry/subjects gets a clean 404 instead
        // of an endpoint that always returns an error.
        val schemaRegistry = pipeline.context()
            .getProviderAs<SchemaRegistryProvider?>(SchemaRegistryProvider.NAME, SchemaRegistryProvider::class.java)
        if (schemaRegistry != null) {
            SchemaRegistryHandler(schemaRegistry).mapRoutes(app)
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
        if (app != null) {
            app!!.stop()
            app = null
            boundPort = -1
        }
        // Release plugin classloaders (both the reload-replaced current
        // one and the startup-supplied one). Safe if either is null.
        if (currentPlugins != null) {
            currentPlugins!!.close()
            currentPlugins = null
        }
        plugins.close()
    }

    // --- Handlers ---
    private fun handleIngest(ctx: Context) {
        val body = ctx.bodyAsBytes()
        val attributes: MutableMap<String?, String?> = HashMap<String?, String?>()
        ctx.headerMap().forEach { (k: String?, v: String?) ->
            if (k == null) return@forEach
            val lower = k.lowercase(Locale.getDefault())
            if (lower.startsWith("x-flow-")) {
                attributes.put(lower.substring("x-flow-".length), v)
            }
        }
        val ff = FlowFile.create(body, attributes)
        try {
            pipeline.ingest(ff)
            ctx.status(202).result(ff.stringId())
        } catch (ex: RuntimeException) {
            log.error("ingest failed for {}: {}", ff.stringId(), ex.toString(), ex)
            ctx.status(500).result("pipeline error: " + ex.message)
        }
    }

    /** UI-facing flowfile injection. Accepts JSON
     * {target, content|contentBase64, attributes} and pushes a synthetic
     * FlowFile through the graph. Mirrors the C# `POST /api/flowfiles/ingest`
     * so the shared React UI's test-flowfile dialog works on both tracks. */
    @Throws(Exception::class)
    private fun handleIngestFlowFile(ctx: Context) {
        val body: MutableMap<String?, Any?>?
        try {
            body = json.readValue<MutableMap<*, *>?>(ctx.bodyAsBytes(), MutableMap::class.java)
        } catch (ex: Exception) {
            writeError(ctx, 400, "body must be a JSON object: " + ex.message)
            return
        }
        if (body == null) {
            writeError(ctx, 400, "body must be a JSON object")
            return
        }

        val data: ByteArray?
        val b64 = body.get("contentBase64")
        if (b64 is String && !b64.isEmpty()) {
            try {
                data = Base64.getDecoder().decode(b64)
            } catch (ex: IllegalArgumentException) {
                writeError(ctx, 400, "contentBase64 invalid: " + ex.message)
                return
            }
        } else if (body.get("content") is String) {
            data = s.toByteArray(StandardCharsets.UTF_8)
        } else {
            data = ByteArray(0)
        }

        val attrs: MutableMap<String?, String?> = HashMap<String?, String?>()
        val a = body.get("attributes")
        if (a is MutableMap<*, *>) {
            for (e in a.entries) {
                if (e.key != null) attrs.put(e.key.toString(), if (e.value == null) "" else e.value.toString())
            }
        }

        val target = if (body.get("target") is String) t else ""
        val ff = FlowFile.create(data, attrs)

        try {
            if (target.isEmpty() || "*" == target) {
                pipeline.ingest(ff)
                ctx.contentType("application/json").result(
                    json.writeValueAsBytes(
                        Map.of<String?, String?>(
                            "status", "ingested",
                            "flowfile", ff.stringId(),
                            "target", "entry-points"
                        )
                    )
                )
            } else {
                pipeline.ingestAt(ff, target)
                ctx.contentType("application/json").result(
                    json.writeValueAsBytes(
                        Map.of<String?, String?>(
                            "status", "ingested",
                            "flowfile", ff.stringId(),
                            "target", target
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
        if (name == null || name.isEmpty()) {
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
                Map.of<String?, String?>(
                    "status", "reset",
                    "name", name
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
    private fun summaryStats(): MutableMap<String?, Any?> {
        val out: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        out.put("processed", pipeline.stats().snapshot().get("totalProcessed") as Long?)
        out.put("activeExecutions", pipeline.metrics().activeExecutions())
        out.put("processors", pipeline.graph().processors.size)
        out.put("sources", pipeline.sources().size)
        return out
    }

    @Throws(Exception::class)
    private fun handleProcessors(ctx: Context) {
        val graph = pipeline.graph()
        val out: MutableMap<String?, String?> = LinkedHashMap<String?, String?>()
        for (entry in graph.processors.entries) {
            out.put(entry.key, pipeline.processorType(entry.key))
        }
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
        val processors: MutableList<MutableMap<String?, Any?>?> = ArrayList<MutableMap<String?, Any?>?>()
        val perProcStats = pipeline.processorStats()
        for (entry in graph.processors.entries) {
            val name = entry.key
            val info: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
            info.put("name", name)
            info.put(ConfigLoader.Companion.TYPE_KEY, pipeline.processorType(name))
            info.put("state", pipeline.processorState(name).name)
            info.put("config", pipeline.processorConfig(name))
            info.put("stats", perProcStats.getOrDefault(name, Map.of<String?, Long?>()))
            info.put("connections", graph.connections.getOrDefault(name, Map.of<String?, MutableList<String?>?>()))
            processors.add(info)
        }

        val providers: MutableList<MutableMap<String?, Any?>?> = ArrayList<MutableMap<String?, Any?>?>()
        val pctx = pipeline.context()
        for (pname in pctx.listProviders()) {
            val p = pctx.getProvider(pname)
            providers.add(
                Map.of<String?, Any?>(
                    "name", pname,
                    "type", if (p == null) "unknown" else p.providerType(),
                    "state", if (p == null) "UNKNOWN" else p.state().name
                )
            )
        }

        val srcs: MutableList<MutableMap<String?, Any?>?> = ArrayList<MutableMap<String?, Any?>?>()
        for (s in pipeline.sources().values) {
            srcs.add(
                Map.of<String?, Any?>(
                    "name", s.name(),
                    "type", s.sourceType(),
                    "running", s.isRunning
                )
            )
        }

        val out: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        out.put("entryPoints", graph.entryPoints)
        out.put("processors", processors)
        // Top-level `connections` is a Java-track add-on (the current UI's
        // FlowController + BfsLayout read from here). C# doesn't expose
        // it; the same data is inline on each processor. Tracked as a
        // C#-cohort candidate for consistency.
        out.put("connections", graph.connections)
        out.put("providers", providers)
        out.put("sources", srcs)
        // Embedded stats match /api/stats summary shape (processed,
        // activeExecutions, processors, sources) — not the detailed
        // Stats.snapshot(). Dashboards only need the summary; callers
        // that want totals/errors/drops go to /api/processor-stats.
        out.put("stats", summaryStats())
        ctx.contentType("application/json").result(json.writeValueAsBytes(out))
    }

    @Throws(Exception::class)
    private fun handleRegistry(ctx: Context) {
        val r = pipeline.registry()
        val out: MutableList<MutableMap<String?, Any?>?> = ArrayList<MutableMap<String?, Any?>?>()
        if (r != null) {
            // Emit the latest version of each type, matching the C# worker's
            // shape so the shared React UI consumes both uniformly.
            val latestByName = TreeMap<String?, Registry.TypeInfo>()
            for (info in r.listAll()) {
                val prev = latestByName.get(info.name)
                if (prev == null || TypeRefs.compareVersions(info.version, prev.version) > 0) {
                    latestByName.put(info.name, info)
                }
            }
            for (info in latestByName.values) {
                out.add(typeInfoToJson(info))
            }
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(out))
    }

    private fun handleMetrics(ctx: Context) {
        ctx.contentType("text/plain; version=0.0.4; charset=utf-8")
            .result(pipeline.metrics().scrape())
    }

    private fun handleDashboard(ctx: Context) {
        try {
            HttpServer::class.java.getClassLoader().getResourceAsStream("dashboard.html").use { `in` ->
                if (`in` == null) {
                    ctx.status(404).result("dashboard.html not on classpath")
                    return
                }
                ctx.contentType("text/html; charset=utf-8").result(`in`.readAllBytes())
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
            ctx.status(200)
                .contentType("application/json")
                .result(
                    json.writeValueAsBytes(
                        Map.of(
                            "status", "reloaded",
                            "added", diff.added,
                            "removed", diff.removed,
                            "updated", diff.updated,
                            "connectionsChanged", diff.connectionsChanged,
                            "total", diff.total()
                        )
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
        val srcs: MutableList<MutableMap<String?, Any?>?> = ArrayList<MutableMap<String?, Any?>?>()
        for (s in pipeline.sources().values) {
            srcs.add(Map.of<String?, Any?>("name", s.name(), "type", s.sourceType(), "running", s.isRunning))
        }
        ctx.contentType("application/json").result(
            json.writeValueAsBytes(
                Map.of<String?, Any?>(
                    "status", "healthy",
                    "sources", srcs
                )
            )
        )
    }

    // --- Providers ---
    @Throws(Exception::class)
    private fun handleProviders(ctx: Context) {
        val out: MutableList<MutableMap<String?, Any?>?> = ArrayList<MutableMap<String?, Any?>?>()
        val pctx = pipeline.context()
        for (name in pctx.listProviders()) {
            val p = pctx.getProvider(name)
            out.add(
                Map.of<String?, Any?>(
                    "name", name,
                    "type", if (p == null) "unknown" else p.providerType(),
                    "state", if (p == null) "UNKNOWN" else p.state().name
                )
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
        if (nStr != null && !nStr.isEmpty()) {
            try {
                n = nStr.toInt()
            } catch (e: NumberFormatException) {
                writeError(ctx, 400, "query param 'n' is not an integer: '" + nStr + "'")
                return
            }
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(Companion.shape(prov.getRecent(n))))
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
        } catch (e: NumberFormatException) {
            writeError(ctx, 400, "path param 'id' is not a long")
            return
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(Companion.shape(prov.getEvents(id))))
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
            val name = body["name"].takeIf { it?.toString()?.isNotEmpty() == true } ?: throw IllegalArgumentException("Name is required but not provided")
            val type = body[ConfigLoader.TYPE_KEY].takeIf { it?.toString()?.isNotEmpty() == true } ?: throw IllegalArgumentException("Type is required but not provided")

//        val name: String = str(body["name"])
//            val type: String = str(body[ConfigLoader.TYPE_KEY])
//            if (name.isEmpty() || type.isEmpty()) {
//                writeError(ctx, 400, "name and type required")
//                return
//            }

            val config: MutableMap<String, String> = asStringMap(body[ConfigLoader.CONFIG_KEY])
            val requires: MutableList<String> = asStringList(body["requires"])
            val connections: MutableMap<String, MutableList<String>> = asConnections(body["connections"])

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
                Map.of<String?, String?>(
                    "name", name,
                    "state", pipeline.processorState(name).name
                )
            )
        )
    }

    // --- Sources ---
    @Throws(Exception::class)
    private fun handleSources(ctx: Context) {
        val out: MutableList<MutableMap<String?, Any?>?> = ArrayList<MutableMap<String?, Any?>?>()
        for (s in pipeline.sources().values) {
            out.add(Map.of<String?, Any?>("name", s.name(), "type", s.sourceType(), "running", s.isRunning))
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
    @Throws(Exception::class)
    private fun handleAddConnection(ctx: Context) {
        val body = readJsonBody(ctx)
        if (body == null) {
            writeError(ctx, 400, "invalid json body")
            return
        }
        val from: String = str(body.get("from"))
        val rel: String = str(body.get("relationship"))
        val to: String = str(body.get("to"))
        writeEditResult(
            ctx, pipeline.addConnection(from, rel, to),
            Map.of<String?, Any?>("status", "added", "from", from, "relationship", rel, "to", to)
        )
    }

    @Throws(Exception::class)
    private fun handleRemoveConnection(ctx: Context) {
        val body = readJsonBody(ctx)
        if (body == null) {
            writeError(ctx, 400, "invalid json body")
            return
        }
        val from: String = str(body.get("from"))
        val rel: String = str(body.get("relationship"))
        val to: String = str(body.get("to"))
        writeEditResult(
            ctx, pipeline.removeConnection(from, rel, to),
            Map.of<String?, Any?>("status", "removed", "from", from, "relationship", rel, "to", to)
        )
    }

    @Throws(Exception::class)
    private fun handleSetConnections(ctx: Context) {
        val from = ctx.pathParam("from")
        val body = readJsonBody(ctx)
        if (body == null) {
            writeError(ctx, 400, "invalid json body")
            return
        }
        val rels: MutableMap<String?, MutableList<String?>?> = LinkedHashMap<String?, MutableList<String?>?>()
        for (entry in body.entries) {
            if (entry.value is MutableList<*>) {
                val targets: MutableList<String?> = ArrayList<String?>(list.size)
                for (o in list) targets.add(str(o))
                rels.put(entry.key, targets)
            } else {
                writeError(ctx, 400, "relationship '" + entry.key + "' must map to a list of target names")
                return
            }
        }
        writeEditResult(
            ctx, pipeline.setConnections(from, rels),
            Map.of<String?, Any?>("status", "replaced", "from", from, "relationships", rels)
        )
    }

    @Throws(Exception::class)
    private fun handleSetEntryPoints(ctx: Context) {
        val body = readJsonBody(ctx)
        if (body == null) {
            writeError(ctx, 400, "invalid json body")
            return
        }
        val namesObj = body.get("names")
        if (namesObj !is MutableList<*>) {
            writeError(ctx, 400, "body must include a 'names' array")
            return
        }
        val names: MutableList<String?> = ArrayList<String?>(namesObj.size)
        for (o in namesObj) names.add(str(o))
        writeEditResult(
            ctx, pipeline.setEntryPoints(names),
            Map.of<String?, Any?>("status", "replaced", "names", names)
        )
    }

    @Throws(Exception::class)
    private fun writeEditResult(ctx: Context, result: EditResult, okBody: MutableMap<String?, Any?>?) {
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
            .getProviderAs<VersionControlProvider?>(VersionControlProvider.NAME, VersionControlProvider::class.java)
    }

    @Throws(Exception::class)
    private fun handleVcStatus(ctx: Context) {
        val v = vc()
        if (v == null || !v.isEnabled()) {
            ctx.contentType("application/json").result(
                json.writeValueAsBytes(
                    Map.of<String?, Boolean?>("enabled", false)
                )
            )
            return
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(v.statusJson()))
    }

    @Throws(Exception::class)
    private fun handleVcCommit(ctx: Context) {
        val v = vc()
        if (v == null || !v.isEnabled()) {
            writeError(ctx, 503, "version control provider not enabled")
            return
        }
        val body = readJsonBody(ctx)
        if (body == null) {
            writeError(ctx, 400, "invalid json body")
            return
        }
        val message: String = str(body.get("message"))
        if (message.isEmpty()) {
            writeError(ctx, 400, "commit message must not be blank")
            return
        }
        val path: String? = if (body.get("path") == null) null else str(body.get("path"))

        val r = v.commit(path, message)
        val out: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        out.put("ok", r.ok)
        out.put("exitCode", r.exitCode)
        out.put("stdout", r.stdout())
        out.put("stderr", r.stderr())
        ctx.status(if (r.ok) 200 else 500).contentType("application/json")
            .result(json.writeValueAsBytes(out))
    }

    @Throws(Exception::class)
    private fun handleVcPush(ctx: Context) {
        val v = vc()
        if (v == null || !v.isEnabled()) {
            writeError(ctx, 503, "version control provider not enabled")
            return
        }
        val r = v.push()
        val out: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        out.put("ok", r.ok)
        out.put("exitCode", r.exitCode)
        out.put("stdout", r.stdout())
        out.put("stderr", r.stderr())
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
        if (nStr != null && !nStr.isEmpty()) {
            try {
                n = nStr.toInt()
            } catch (e: NumberFormatException) {
                writeError(ctx, 400, "query param 'n' is not an integer: '" + nStr + "'")
                return
            }
        }
        // Over-fetch to give the filter a real chance of returning n
        // failures even when non-failure events dominate the ring buffer.
        val window = max(n * 20, 500)
        val recent: MutableList<ProvenanceProvider.Event> = prov.getRecent(window)
        val failures: MutableList<ProvenanceProvider.Event> = ArrayList<ProvenanceProvider.Event>()
        for (e in recent) {
            if (e.type == ProvenanceProvider.EventType.FAILED) failures.add(e)
            if (failures.size >= n) break
        }
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
        val type: String? =
            if (body.get(ConfigLoader.Companion.TYPE_KEY) == null) null else str(body.get(ConfigLoader.Companion.TYPE_KEY))
        val config: MutableMap<String?, String?> = asStringMap(body.get(ConfigLoader.Companion.CONFIG_KEY))

        val r = pipeline.updateProcessorConfig(name, type, config)
        writeEditResult(ctx, r, Map.of<String?, Any?>("status", "updated", "name", name))
    }

    @Throws(Exception::class)
    private fun handleSetProcessorConnections(ctx: Context) {
        val name = ctx.pathParam("name")
        val body = readJsonBody(ctx)
        if (body == null) {
            writeError(ctx, 400, "invalid json body")
            return
        }
        val rels: MutableMap<String?, MutableList<String?>?> = LinkedHashMap<String?, MutableList<String?>?>()
        for (entry in body.entries) {
            if (entry.value is MutableList<*>) {
                val targets: MutableList<String?> = ArrayList<String?>(list.size)
                for (o in list) targets.add(str(o))
                rels.put(entry.key, targets)
            } else {
                writeError(ctx, 400, "relationship '" + entry.key + "' must map to a list of target names")
                return
            }
        }
        val r = pipeline.setConnections(name, rels)
        writeEditResult(ctx, r, Map.of<String?, Any?>("status", "replaced", "from", name, "relationships", rels))
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
            Files.writeString(configPath, yaml)
        } catch (ex: IOException) {
            writeError(ctx, 500, "flow save failed: " + ex.message)
            return
        }
        log.info("flow saved to {}", configPath)

        val body: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        body.put("status", "saved")
        body.put("path", configPath.toString())
        body.put("bytes", yaml.length)
        body.put("committed", false)
        body.put("pushed", false)

        // VC-aware save: if VersionControlProvider is enabled, also
        // stage+commit the config file and optionally push to the
        // configured remote. One UI button → one backend call → right
        // thing happens. Matches zinc-flow-csharp's POST /api/flow/save
        // behavior so the UI sees the same response shape on both tracks.
        val vc =
            pipeline.context().getProviderAs<VersionControlProvider?>(
                "version_control",
                VersionControlProvider::class.java
            )
        if (vc != null && vc.isEnabled()) {
            val reqBody = readJsonBody(ctx)
            var message = "flow: update via UI"
            var push = true
            if (reqBody != null) {
                val m = reqBody.get("message")
                if (m is String && !m.isBlank()) message = m
                val p = reqBody.get("push")
                if (p is Boolean) push = p
            }
            val relPath = configPath.getFileName().toString()
            val commitRes = vc.commit(relPath, message)
            body.put("committed", commitRes.ok)
            body.put("commitExitCode", commitRes.exitCode)
            body.put("commitStdout", commitRes.stdout())
            if (!commitRes.ok) body.put("commitStderr", commitRes.stderr())

            if (commitRes.ok && push) {
                val pushRes = vc.push()
                body.put("pushed", pushRes.ok)
                body.put("pushExitCode", pushRes.exitCode)
                body.put("pushStdout", pushRes.stdout())
                if (!pushRes.ok) body.put("pushStderr", pushRes.stderr())
            }
        }

        ctx.status(200).contentType("application/json").result(json.writeValueAsBytes(body))
    }

    // --- Processor types (versioned registry) ---
    @Throws(Exception::class)
    private fun handleProcessorTypes(ctx: Context) {
        val r = pipeline.registry()
        val out: MutableList<MutableMap<String?, Any?>?> = ArrayList<MutableMap<String?, Any?>?>()
        if (r != null) {
            for (info in r.listAll()) {
                out.add(typeInfoToJson(info))
            }
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(out))
    }

    @Throws(Exception::class)
    private fun handleProcessorType(ctx: Context) {
        val r = pipeline.registry()
        if (r == null) {
            writeError(ctx, 503, "registry not wired — processor types unavailable")
            return
        }
        val name = ctx.pathParam("name")
        val versions = r.listVersions(name)
        if (versions.isEmpty()) {
            writeError(ctx, 404, "processor type '" + name + "' not found")
            return
        }
        val body: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        body.put("name", name)
        body.put("latest", if (r.latest(name) == null) null else r.latest(name).version)
        val vs: MutableList<MutableMap<String?, Any?>?> = ArrayList<MutableMap<String?, Any?>?>()
        for (info in versions) vs.add(typeInfoToJson(info))
        body.put("versions", vs)
        ctx.contentType("application/json").result(json.writeValueAsBytes(body))
    }

    // --- Overlays ---
    @Throws(Exception::class)
    private fun handleOverlays(ctx: Context) {
        val resolved = if (loader == null) null else loader.lastOverlay()
        if (resolved == null) {
            writeError(ctx, 503, "overlay info unavailable — server started without a config loader")
            return
        }
        val layers: MutableList<MutableMap<String?, Any?>?> = ArrayList<MutableMap<String?, Any?>?>()
        for (layer in resolved.layers) {
            val info: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
            info.put("role", layer.role)
            info.put("path", if (layer.path == null) null else layer.path.toString())
            info.put("present", layer.present)
            info.put("size", layer.content.size)
            layers.add(info)
        }
        val body: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        body.put("base", if (resolved.basePath == null) null else resolved.basePath.toString())
        body.put("layers", layers)
        body.put("effective", resolved.effective)
        body.put("provenance", resolved.provenance)
        ctx.contentType("application/json").result(json.writeValueAsBytes(body))
    }

    // --- Plugins ---
    @Volatile
    private var currentPlugins: PluginLoader.Summary? = null

    /** Full constructor — wires in the plugin summary and node identity
     * so `/api/plugins` and `/api/identity` can answer. */
    /** Constructor for the config-driven path — enables `/api/reload`. */
    init {
        this.plugins = if (plugins == null) PluginLoader.Summary.Companion.empty() else plugins
    }

    private fun pluginSummary(): PluginLoader.Summary? {
        return if (currentPlugins != null) currentPlugins else plugins
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
        if (pipeline.registry() == null) {
            writeError(ctx, 501, "pipeline has no registry wired — plugin reload unavailable")
            return
        }
        // Release the previous URLClassLoader before replacing it — if
        // we leak these, repeated reloads pin every plugin jar open
        // and exhaust file handles over time.
        val prior = currentPlugins
        currentPlugins = loadFromDirectory(pluginsDir, pipeline.registry(), pipeline.context())
        if (prior != null) prior.close()
        log.info("reloaded plugins from {} — {} loaded", pluginsDir, currentPlugins!!.totalLoaded())
        ctx.contentType("application/json")
            .result(json.writeValueAsBytes(PluginLoader.toJson(currentPlugins)))
    }

    // --- Body + response helpers ---
    private fun readJsonBody(ctx: Context): MutableMap<String, Any>? {
        return try {
            json.readValue(ctx.bodyAsBytes(), MutableMap::class.java)
        } catch (e: Exception) {
            null
        }
    }

    private fun nameFromBody(ctx: Context): String {
        val body = readJsonBody(ctx)
        return if (body == null) "" else str(body.get("name"))
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
            .result(json.writeValueAsBytes(Map.of<String?, String?>("error", message)))
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

        private fun shape(events: MutableList<ProvenanceProvider.Event>): MutableList<MutableMap<String?, Any?>?> {
            val out: MutableList<MutableMap<String?, Any?>?> = ArrayList<MutableMap<String?, Any?>?>(events.size)
            for (e in events) {
                out.add(
                    Map.of<String?, Any?>(
                        "flowFileId", e.flowFileId,
                        "type", e.type!!.name,
                        "component", e.component,
                        "details", e.details,
                        "timestampMillis", e.timestampMillis
                    )
                )
            }
            return out
        }

        private fun typeInfoToJson(info: Registry.TypeInfo): MutableMap<String, Any> {
            return buildMap {
                put("name", info.name)
                put("version", info.version)
                put("description", info.description)
                put("category", info.category)
                put("configKeys", info.configKeys)
                put("relationships", info.relationships)
                put("parameters", info.parameters?.filterNotNull()?.map { paramInfoToJson(it) })
            }.filterValues { it != null }.toMutableMap() as MutableMap<String, Any>
        }

        private fun paramInfoToJson(p: ParamInfo): MutableMap<String, Any> {
            return buildMap {
                put("name", p.name)
                put("label", p.label)
                put("description", p.description)
                put("kind", p.kind?.jsonName())
                put("required", p.required)
                put("default", p.defaultValue)
                put("placeholder", p.placeholder)
                put("choices", p.choices)
                put("valueKind", p.valueKind?.jsonName())
                put("entryDelim", p.entryDelim)
                put("pairDelim", p.pairDelim)
            }.filterValues { it != null }.toMutableMap() as MutableMap<String, Any>
        }

        private fun str(o: Any?): String {
            return o?.toString() ?: ""
        }

        private fun asStringMap(raw: Any?): MutableMap<String, String> {
            if (raw !is MutableMap<*, *>) return mutableMapOf()
            return raw.entries.associate { (key, value) -> key.toString() to str(value) }.toMutableMap()
        }

        private fun asStringList(raw: Any?): MutableList<String> {
            if (raw !is MutableList<*>) return mutableListOf()
            return raw.map { str(it) }.toMutableList()
        }

        private fun asConnections(raw: Any?): MutableMap<String, MutableList<String>> {
            val map = raw as? MutableMap<*, *> ?: return mutableMapOf()
            return map.entries.associateTo(mutableMapOf()) { (key, value) ->
                val relationship = key.toString()
                val targets = when(value) {
                    is List<*> -> value.map { it.toString() }
                    null -> emptyList()
                    else -> listOf(value.toString())
                }.toMutableList()
                relationship to targets
            }
        }
    }
}
