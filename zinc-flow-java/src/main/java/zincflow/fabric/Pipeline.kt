package zincflow.fabric

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import zincflow.core.ComponentState
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorContext
import zincflow.core.ProcessorResult
import zincflow.core.Relationships
import zincflow.core.Source
import zincflow.providers.ProvenanceProvider
import java.util.ArrayDeque
import java.util.Deque
import java.util.List
import java.util.Map
import java.util.Objects
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Predicate
import java.util.function.Supplier
import kotlin.collections.emptyList
import kotlin.concurrent.Volatile

typealias ConnectionMap = MutableMap<String, MutableMap<String, MutableList<String>>>
typealias RelationshipMap = MutableMap<String, MutableList<String>>
/** Direct pipeline executor — iterative, depth-first, work-stack. No
 * inter-stage queues. Each call to [.ingest] runs the
 * FlowFile through the graph on the calling thread; concurrency comes
 * from having multiple source threads call `ingest` in parallel.
 * 
 * Matches the zinc-flow-csharp model — see
 * `zinc-flow-csharp/ZincFlow/Fabric/Fabric.cs`. */
class PipelineKt @JvmOverloads constructor(
    @Volatile private var graph: PipelineGraphKt,
    private val maxHops: Int = DEFAULT_MAX_HOPS,
    metrics: Metrics? = null,
    private val context: ProcessorContext = ProcessorContext(),
    private val registry: Registry? = null
) {
    private val stats: Stats = Stats(metrics)
//    private val context: ProcessorContext

    // Per-processor lifecycle state. Processors added via the API land
    // in ENABLED; disabled processors cause drain() to short-circuit
    // their slot and drop the FlowFile. Populated lazily — a missing
    // entry means ENABLED (matches the original pre-3e behaviour where
    // every processor always ran).
    private val processorStates = ConcurrentHashMap<String, ComponentState>()

    // Record of how each processor was constructed — stored so the admin
    // API can expose the current shape of the graph. Entries are only
    // present for processors created through addProcessor / the config
    // loader's Fabric wiring; processors constructed ad-hoc (unit tests)
    // have no entry and the API reports type="unknown".
    private val processorDefs = ConcurrentHashMap<String, ProcessorDef>()
    private val sources = ConcurrentHashMap<String, Source>()

    fun metrics(): Metrics {
        return stats.metrics()
    }

    fun context(): ProcessorContext {
        return context
    }

    fun registry(): Registry? {
        return registry
    }

    fun provenance(): ProvenanceProvider? {
        return context.getProviderAs(ProvenanceProvider.NAME, ProvenanceProvider::class.java)
    }

    /** Non-null provenance for the drain hot path — falls back to the
     * shared disabled singleton when no provenance is wired, so the
     * inner loop can call `record(...)` unconditionally without
     * a per-event null check. `record` itself early-returns when
     * the provider is disabled. */
    private fun provenanceOrNoop(): ProvenanceProvider {
        return provenance() ?: ProvenanceProvider.DISABLED
    }

    /** Record how a processor was constructed. Called by the Fabric
     * wiring (ConfigLoader / addProcessor) after the factory runs so
     * the admin API can later answer "what type is X?". */
    fun recordProcessorDef(
        name: String,
        type: String,
        config: MutableMap<String, String>,
        requires: MutableList<String>
    ) {
        processorDefs[name] = ProcessorDef(type, config.toMutableMap(), requires.toMutableList())
        processorStates.putIfAbsent(name, ComponentState.ENABLED)
    }

    /** Construct a processor through the registry and add it to the
     * running graph. Rejects duplicates. Returns false if no registry
     * is wired or the name is already taken. Matches C# Fabric.AddProcessor. */
    fun addProcessor(
        name: String,
        type: String,
        config: MutableMap<String, String>,
        requires: MutableList<String>,
        connections: MutableMap<String, MutableList<String>>
    ): Boolean {
        if (registry == null) return false
        if (name.isEmpty()) return false
        val g = graph
        if (g.processors.containsKey(name)) return false
        if (!registry.has(type)) return false

        val req = requires.toMutableList()
        for (provName in req) context.registerDependent(provName, name)

        val proc = registry.create(type, config, context) ?: return false

        val newProcessors = g.processors.toMutableMap()
        newProcessors[name] = proc
        val newConnections = g.connections.toMutableMap()
        if (connections.isNotEmpty()) {
            val copy: MutableMap<String, MutableList<String>> = mutableMapOf()
            connections.forEach { (k: String, v: MutableList<String>) -> copy[k] = v.toMutableList() }
            newConnections[name] = copy
        }
        graph = PipelineGraphKt(newProcessors, newConnections, g.entryPoints)
        recordProcessorDef(name, type, config, req)
        return true
    }

    fun removeProcessor(name: String): Boolean {
        val g = graph
        if (!g.processors.containsKey(name)) return false
        val newProcessors = g.processors.filter { it.key == name }.toMutableMap()
        val newConnections = g.connections.filter { it.key == name }.toMutableMap()
        val newEntries: MutableList<String> = ArrayList(g.entryPoints)
        newEntries.remove(name)
        graph = PipelineGraphKt(newProcessors, newConnections, newEntries)
        processorStates.remove(name)
        processorDefs.remove(name)
        return true
    }

    fun enableProcessor(name: String): Boolean {
        if (!graph.processors.containsKey(name)) return false
        processorStates[name] = ComponentState.ENABLED
        return true
    }

    /** Rebuild a running processor with a new config. Keeps its
     * connections + name + lifecycle state; replaces the instance
     * atomically. Rejects unknown processor names and unknown types.
     * Type override is optional — when null, the current type from
     * [.processorDefs] (or the config loader) is reused. */
    fun updateProcessorConfig(name: String, type: String, config: MutableMap<String, String>): EditResult {
        if (registry == null) return EditResult.fail("no registry wired")
        if (name.isEmpty()) return EditResult.fail("name must not be blank")
        val g = graph
        if (!g.processors.containsKey(name)) return EditResult.fail("processor '$name' not found")

        val effectiveType = type.ifEmpty { processorType(name) }
        if (effectiveType == null || "unknown" == effectiveType) {
            return EditResult.fail("cannot determine type for '$name' — pass 'type' explicitly")
        }
        if (!registry.has(effectiveType)) {
            return EditResult.fail("unknown processor type '$effectiveType'")
        }

        val cfg = Map.copyOf<String, String>(config)
        val rebuilt = registry.create(effectiveType, cfg, context) ?: return EditResult.fail("Could not rebuild processor")

        val newProcessors: MutableMap<String, Processor> = g.processors.toMutableMap()
        newProcessors[name] = rebuilt
        graph = PipelineGraphKtt.of(newProcessors, g.connections, g.entryPoints)

        val prior = processorDefs[name]
        val requires = prior?.requires ?: mutableListOf()
        recordProcessorDef(name, effectiveType, cfg, requires)
        return EditResult.success()
    }

    // --- Connection-edge mutations ---
    /** Result of a single graph-edit call. `ok == false` means
     * the edit was rejected (unknown processor, duplicate edge, etc.);
     * `reason` carries a short human-readable explanation so the
     * admin API can echo it. */
    @JvmRecord
    data class EditResult(@JvmField val ok: Boolean, @JvmField val reason: String?) {
        companion object {
            fun success(): EditResult {
                return EditResult(true, "")
            }

            fun fail(reason: String?): EditResult {
                return EditResult(false, reason)
            }
        }
    }

    /** Add a single outbound connection. Rejects unknown processors on
     * either end and duplicates of an edge that already exists. All
     * other cases build a new graph and swap atomically. */
    fun addConnection(from: String?, relationship: String?, to: String?): EditResult {
        if (from.isNullOrEmpty()) return EditResult.fail("from must not be blank")
        if (relationship.isNullOrEmpty()) return EditResult.fail("relationship must not be blank")
        if (to.isNullOrEmpty()) return EditResult.fail("to must not be blank")
        val g = graph
        if (!g.processors.containsKey(from)) return EditResult.fail("processor '$from' not found")
        if (!g.processors.containsKey(to)) return EditResult.fail("processor '$to' not found")

        val newConnections: ConnectionMap = g.connections.toMutableMap()
        val newRelationships: RelationshipMap = newConnections.getOrDefault(from, emptyMap()).toMutableMap()
        val targets: MutableList<String> = newRelationships.getOrDefault(relationship, emptyList()).toMutableList()
        if (targets.contains(to)) {
            return EditResult.fail("connection '$from:$relationship → $to' already exists")
        }
        targets.add(to)
        newRelationships[relationship] = targets.toMutableList()
        newConnections[from] = newRelationships
        graph = PipelineGraphKt(g.processors, newConnections, g.entryPoints)
        return EditResult.success()
    }

    fun removeConnection(from: String, relationship: String, to: String): EditResult {
        val g = graph
        val relationships = g.connections[from] ?: return EditResult.fail("From connection '$from' not found")
        if(relationships[relationship]?.contains(to) != true) {
            return EditResult.fail("connection '$from:$relationship → $to' not found")
        }
//        if (!relationships.containsKey(relationship) || !relationships[relationship]!!.contains(to)) {
//            return EditResult.fail("connection '$from:$relationship → $to' not found")
//        }
        val newConns: ConnectionMap = g.connections.toMutableMap()
        val newRels: RelationshipMap = relationships.toMutableMap()
        val newTargets: MutableList<String> = newRels[relationship]?.filter { it == to }?.toMutableList() ?: return EditResult.fail("Relationship target '$to' not found")
        if (newTargets.isEmpty()) {
            newRels.remove(relationship)
        } else {
            newRels[relationship] = newTargets
        }
        if (newRels.isEmpty()) {
            newConns.remove(from)
        } else {
            newConns[from] = newRels
        }
        graph = PipelineGraphKt(g.processors, newConns, g.entryPoints)
        return EditResult.success()
    }

    /** Replace every outbound connection of a processor in a single
     * atomic swap. `rels` with an empty map clears the processor's
     * outbound connections entirely. */
    fun setConnections(from: String?, rels: MutableMap<String?, MutableList<String?>>?): EditResult {
        var rels = rels
        if (from == null || from.isEmpty()) return EditResult.fail("from must not be blank")
        val g = graph
        if (!g.processors.containsKey(from)) return EditResult.fail("processor '" + from + "' not found")
        if (rels == null) rels = Map.of<String?, MutableList<String?>?>()

        // Validate every target exists before committing.
        for (targets in rels.values) {
            for (t in targets) {
                if (!g.processors.containsKey(t)) {
                    return EditResult.fail("target processor '" + t + "' not found")
                }
            }
        }

        val newConns: MutableMap<String?, MutableMap<String?, MutableList<String?>?>> =
            HashMap<String?, MutableMap<String?, MutableList<String?>?>>(g.connections)
        if (rels.isEmpty()) {
            newConns.remove(from)
        } else {
            val copy: MutableMap<String?, MutableList<String?>?> = LinkedHashMap<String?, MutableList<String?>?>()
            rels.forEach { (rel: String?, ts: MutableList<String?>?) -> copy.put(rel, List.copyOf<String?>(ts)) }
            newConns.put(from, copy)
        }
        graph = PipelineGraphKt(g.processors, newConns, g.entryPoints)
        return EditResult.success()
    }

    /** Replace the set of entry points. Every name must already be a
     * defined processor; otherwise the update is rejected. */
    fun setEntryPoints(names: MutableList<String?>?): EditResult {
        if (names == null) return EditResult.fail("names must not be null")
        val g = graph
        for (name in names) {
            if (!g.processors.containsKey(name)) {
                return EditResult.fail("processor '" + name + "' not found")
            }
        }
        graph = PipelineGraphKt(g.processors, g.connections, List.copyOf<String?>(names))
        return EditResult.success()
    }

    fun disableProcessor(name: String): Boolean {
        if (!graph.processors.containsKey(name)) return false
        processorStates.put(name, ComponentState.DISABLED)
        return true
    }

    fun processorState(name: String?): ComponentState? {
        if (!graph.processors.containsKey(name)) return ComponentState.DISABLED
        return processorStates.getOrDefault(name, ComponentState.ENABLED)
    }

    fun processorType(name: String?): String? {
        val d = processorDefs.get(name)
        if (d != null) return d.type
        val p = graph.processors.get(name)
        return if (p == null) "unknown" else p.javaClass.getSimpleName()
    }

    /** Config map captured when the processor was registered (via YAML
     * load or API create). Returns an empty map if the processor was
     * added without a recorded def — keeps callers (/api/flow, UI
     * drawer) from having to null-check. */
    fun processorConfig(name: String?): MutableMap<String?, String?>? {
        val d = processorDefs.get(name)
        return if (d == null) Map.of<String?, String?>() else d.config
    }

    /** Cascade-disable every processor that declared `requires`
     * on the named provider. Called by [.disableProvider]
     * to match the C# semantics — disabling a provider takes down its
     * consumers so they don't try to use it. */
    fun disableProvider(providerName: String?): Boolean {
        val p = context.getProvider(providerName)
        if (p == null) return false
        for (proc in context.getDependents(providerName)) {
            disableProcessor(proc)
        }
        p.disable(0)
        return true
    }

    fun enableProvider(providerName: String?): Boolean {
        val p = context.getProvider(providerName)
        if (p == null) return false
        p.enable()
        return true
    }

    // --- Sources ---
    fun addSource(source: Source?) {
        if (source == null) return
        sources.put(source.name(), source)
        stats.metrics().onSourceRegistered(source)
    }

    fun getSource(name: String?): Source? {
        return sources.get(name)
    }

    fun sources(): MutableMap<String?, Source?> {
        return Map.copyOf<String?, Source?>(sources)
    }

    fun startSource(name: String?): Boolean {
        val s = sources.get(name)
        if (s == null) return false
        if (!s.isRunning) s.start(Predicate { ff: FlowFile? -> this.ingestFromSource(ff!!) })
        return true
    }

    /** Source-side ingress. Returns `true` on pipeline acceptance,
     * `false` on failure — sources use the return value to
     * decide whether to mark the upstream item consumed. */
    private fun ingestFromSource(ff: FlowFile): Boolean {
        try {
            ingest(ff)
            return true
        } catch (ex: RuntimeException) {
            log.warn("source ingest failed for {}: {}", ff.stringId(), ex.toString())
            return false
        }
    }

    fun stopSource(name: String?): Boolean {
        val s = sources.get(name)
        if (s == null) return false
        if (s.isRunning) s.stop()
        return true
    }

    @JvmRecord
    data class ProcessorDef(
        val type: String,
        val config: MutableMap<String, String>,
        val requires: MutableList<String>
    )

    /** Per-processor stats pulled from [Stats], shaped the same way
     * as the C# `GetProcessorStats` endpoint. One entry per
     * processor currently in the graph. */
    fun processorStats(): MutableMap<String?, MutableMap<String?, Long?>?> {
        val counts = stats.processorCountsSnapshot()
        val errors = stats.processorErrorsSnapshot()
        val out: MutableMap<String?, MutableMap<String?, Long?>?> =
            LinkedHashMap<String?, MutableMap<String?, Long?>?>()
        for (name in graph.processors.keys) {
            out.put(
                name, Map.of<String?, Long?>(
                    "processed", counts.getOrDefault(name, 0L),
                    "errors", errors.getOrDefault(name, 0L)
                )
            )
        }
        return out
    }

    /** Swap the graph atomically. In-flight ingest calls complete against
     * whichever reference they already loaded; subsequent ingests see
     * the new graph. This is the hot-reload hook. */
    fun swapGraph(next: PipelineGraphKt?) {
        this.graph = Objects.requireNonNull<PipelineGraphKt>(next)
    }

    /** Diff the current graph against `next` and report
     * {added, removed, updated, connectionsChanged}. A processor is
     * "updated" when it reappears in the new graph with a different
     * type or a different processor-def recorded; connection changes
     * are tracked per-source. Results inform `/api/reload`'s
     * response so operators can see what actually changed. */
    fun applyReload(next: PipelineGraphKt): ReloadDiff {
        val before = this.graph
        var added = 0
        var removed = 0
        var updated = 0
        var connectionsChanged = 0

        for (name in before.processors.keys) {
            if (!next.processors.containsKey(name)) removed++
        }
        for (entry in next.processors.entries) {
            val name = entry.key
            if (!before.processors.containsKey(name)) {
                added++
                continue
            }
            // Same name in both — decide updated vs connections-only
            if (before.processors.get(name) !== entry.value) {
                updated++
            }
            val oldConns = before.connections.getOrDefault(name, Map.of<String?, MutableList<String?>?>())
            val newConns = next.connections.getOrDefault(name, Map.of<String?, MutableList<String?>?>())
            if (oldConns != newConns) {
                connectionsChanged++
            }
        }

        this.graph = Objects.requireNonNull<PipelineGraphKt>(next)
        return ReloadDiff(added, removed, updated, connectionsChanged)
    }

    @JvmRecord
    data class ReloadDiff(@JvmField val added: Int, @JvmField val removed: Int, @JvmField val updated: Int, @JvmField val connectionsChanged: Int) {
        fun total(): Int {
            return added + removed + updated + connectionsChanged
        }
    }

    fun stats(): Stats {
        return stats
    }

    fun graph(): PipelineGraphKt {
        return graph
    }

    /** Push a FlowFile into the pipeline at each entry point. Returns
     * when all downstream dispatches for this FlowFile have completed.
     * 
     * Bracketed with `beginExecution`/`endExecution` so the
     * `zinc_flow_active_executions` gauge reflects true
     * in-flight work; the finally block guarantees the gauge decrements
     * even if drain throws. */
    fun ingest(ff: FlowFile) {
        val g = graph // single snapshot for this call
        if (g.entryPoints.isEmpty()) {
            log.warn("ingest called but pipeline has no entry points — dropping {}", ff.stringId())
            return
        }
        val m = stats.metrics()
        m.beginExecution()
        try {
            stats.recordIngested()
            // A work item = (processor name, flowfile to hand it). Stack is
            // local per ingest call so concurrent ingests don't share state.
            val stack: Deque<WorkItem> = ArrayDeque<WorkItem>()
            for (entry in g.entryPoints) {
                stack.push(WorkItem(entry, ff))
            }
            drain(g, stack)
        } finally {
            m.endExecution()
        }
    }

    /** Inject a FlowFile at a specific processor rather than fanning out to
     * entry points. Used by the UI's test-flowfile dialog and targeted
     * replay scenarios. Throws [IllegalArgumentException] if the
     * target isn't in the current graph. */
    fun ingestAt(ff: FlowFile, target: String?) {
        val g = graph
        require(g.processors.containsKey(target)) { "unknown target processor: " + target }
        val m = stats.metrics()
        m.beginExecution()
        try {
            stats.recordIngested()
            val stack: Deque<WorkItem> = ArrayDeque<WorkItem>()
            stack.push(WorkItem(target, ff))
            drain(g, stack)
        } finally {
            m.endExecution()
        }
    }

    private fun drain(g: PipelineGraphKt, stack: Deque<WorkItem>) {
        val prov = provenanceOrNoop()
        while (!stack.isEmpty()) {
            val item = stack.pop()
            val input = item.flowFile

            if (input.hopCount >= maxHops) {
                log.error(
                    "maxHops={} exceeded at processor={}, dropping {}",
                    maxHops, item.processor, input.stringId()
                )
                stats.recordFailed(item.processor)
                prov.record(
                    input.id, ProvenanceProvider.EventType.FAILED,
                    item.processor, "maxHops exceeded"
                )
                continue
            }
            val processor = g.processors.get(item.processor)
            if (processor == null) {
                log.error(
                    "unknown processor '{}' referenced by graph — dropping {}",
                    item.processor, input.stringId()
                )
                stats.recordFailed(item.processor)
                prov.record(
                    input.id, ProvenanceProvider.EventType.FAILED,
                    item.processor, "unknown processor"
                )
                continue
            }

            // Skip disabled processors — drop the FlowFile silently.
            if (processorStates.getOrDefault(item.processor, ComponentState.ENABLED) != ComponentState.ENABLED) {
                prov.record(
                    input.id, ProvenanceProvider.EventType.DROPPED,
                    item.processor, "processor disabled"
                )
                stats.recordDropped()
                continue
            }

            val result: ProcessorResult
            try {
                result = processor.process(input)
            } catch (ex: RuntimeException) {
                log.error(
                    "processor '{}' threw while handling {}: {}",
                    item.processor, input.stringId(), ex.toString(), ex
                )
                stats.recordFailed(item.processor)
                prov.record(
                    input.id, ProvenanceProvider.EventType.FAILED,
                    item.processor, ex.message
                )
                dispatchFailure(g, stack, item.processor, input, ex.message)
                continue
            }
            stats.recordProcessed(item.processor)
            prov.record(
                input.id, ProvenanceProvider.EventType.PROCESSED,
                item.processor, ""
            )
            dispatch(g, stack, item.processor, result)
        }
    }

    private fun dispatch(
        g: PipelineGraphKt, stack: Deque<WorkItem>,
        from: String?, result: ProcessorResult
    ) {
        when (result) {
            -> fanOut(g, stack, from, Relationships.SUCCESS, List.of<FlowFile?>(ff.bumpHop()))
            -> {
                for (out in ffs) {
                    fanOut(g, stack, from, Relationships.SUCCESS, List.of<FlowFile?>(out.bumpHop()))
                }
            }

            -> fanOut(g, stack, from, route, List.of<FlowFile?>(ff.bumpHop()))
            -> {
                for (entry in outputs) {
                    fanOut(g, stack, from, entry.route, List.of<FlowFile?>(entry.flowFile.bumpHop()))
                }
            }

            -> stats.recordDropped()
            -> dispatchFailure(g, stack, from, ff, reason)
        }
    }

    private fun dispatchFailure(
        g: PipelineGraphKt, stack: Deque<WorkItem>,
        from: String?, ff: FlowFile, reason: String?
    ) {
        val failureTargets = g.next(from, Relationships.FAILURE)
        if (failureTargets.isEmpty()) {
            log.warn(
                "failure at '{}' with no 'failure' connections — dropping {} (reason: {})",
                from, ff.stringId(), reason
            )
            stats.recordDropped()
            return
        }
        fanOut(g, stack, from, Relationships.FAILURE, List.of<FlowFile?>(ff.bumpHop()))
    }

    private fun fanOut(
        g: PipelineGraphKt, stack: Deque<WorkItem>,
        from: String?, relationship: String?, ffs: MutableList<FlowFile>
    ) {
        val targets = g.next(from, relationship)
        if (targets.isEmpty()) {
            return  // sink / terminal branch
        }
        for (target in targets) {
            for (ff in ffs) {
                stack.push(WorkItem(target, ff))
            }
        }
    }

    @JvmRecord
    private data class WorkItem(val processor: String?, val flowFile: FlowFile)
    companion object {
        private val log: Logger = LoggerFactory.getLogger(Pipeline::class.java)

        /** Hard cap on processor dispatches per FlowFile — catches accidental
         * cycles in the graph so a pipeline misconfiguration can't spin
         * forever. Matches the C# default. */
        const val DEFAULT_MAX_HOPS: Int = 50
    }
}
