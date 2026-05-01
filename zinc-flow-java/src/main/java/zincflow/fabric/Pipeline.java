package zincflow.fabric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.ComponentState;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorContext;
import zincflow.core.ProcessorResult;
import zincflow.core.Relationships;
import zincflow.core.Source;
import zincflow.providers.ProvenanceProvider;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/// Direct pipeline executor — iterative, depth-first, work-stack. No
/// inter-stage queues. Each call to {@link #ingest(FlowFile)} runs the
/// FlowFile through the graph on the calling thread; concurrency comes
/// from having multiple source threads call {@code ingest} in parallel.
///
/// Matches the zinc-flow-csharp model — see
/// {@code zinc-flow-csharp/ZincFlow/Fabric/Fabric.cs}.
public final class Pipeline {

    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);

    /// Hard cap on processor dispatches per FlowFile — catches accidental
    /// cycles in the graph so a pipeline misconfiguration can't spin
    /// forever. Matches the C# default.
    public static final int DEFAULT_MAX_HOPS = 50;

    private volatile PipelineGraphKt graph;
    private final Stats stats;
    private final int maxHops;
    private final ProcessorContext context;
    private final Registry registry;
    // Per-processor lifecycle state. Processors added via the API land
    // in ENABLED; disabled processors cause drain() to short-circuit
    // their slot and drop the FlowFile. Populated lazily — a missing
    // entry means ENABLED (matches the original pre-3e behaviour where
    // every processor always ran).
    private final ConcurrentHashMap<String, ComponentState> processorStates = new ConcurrentHashMap<>();
    // Record of how each processor was constructed — stored so the admin
    // API can expose the current shape of the graph. Entries are only
    // present for processors created through addProcessor / the config
    // loader's Fabric wiring; processors constructed ad-hoc (unit tests)
    // have no entry and the API reports type="unknown".
    private final ConcurrentHashMap<String, ProcessorDef> processorDefs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Source> sources = new ConcurrentHashMap<>();

    public Pipeline(PipelineGraphKt graph) {
        this(graph, DEFAULT_MAX_HOPS, null, null, null);
    }

    public Pipeline(PipelineGraphKt graph, int maxHops) {
        this(graph, maxHops, null, null, null);
    }

    public Pipeline(PipelineGraphKt graph, int maxHops, Metrics metrics) {
        this(graph, maxHops, metrics, null, null);
    }

    public Pipeline(PipelineGraphKt graph, int maxHops, Metrics metrics,
                    ProcessorContext context, Registry registry) {
        this.graph = Objects.requireNonNull(graph);
        this.maxHops = maxHops;
        this.stats = new Stats(metrics);
        this.context = Objects.requireNonNullElseGet(context, ProcessorContext::new);
        this.registry = registry;
    }

    public Metrics metrics() {
        return stats.metrics();
    }

    public ProcessorContext context() { return context; }

    public Registry registry() { return registry; }

    public ProvenanceProvider provenance() {
        return context.getProviderAs(ProvenanceProvider.NAME, ProvenanceProvider.class);
    }

    /// Non-null provenance for the drain hot path — falls back to the
    /// shared disabled singleton when no provenance is wired, so the
    /// inner loop can call {@code record(...)} unconditionally without
    /// a per-event null check. {@code record} itself early-returns when
    /// the provider is disabled.
    private ProvenanceProvider provenanceOrNoop() {
        ProvenanceProvider p = provenance();
        return p != null ? p : ProvenanceProvider.DISABLED;
    }

    /// Record how a processor was constructed. Called by the Fabric
    /// wiring (ConfigLoader / addProcessor) after the factory runs so
    /// the admin API can later answer "what type is X?".
    public void recordProcessorDef(String name, String type, Map<String, String> config, List<String> requires) {
        if (name == null || type == null) return;
        processorDefs.put(name, new ProcessorDef(
                type,
                config == null ? Map.of() : Map.copyOf(config),
                requires == null ? List.of() : List.copyOf(requires)));
        processorStates.putIfAbsent(name, ComponentState.ENABLED);
    }

    /// Construct a processor through the registry and add it to the
    /// running graph. Rejects duplicates. Returns false if no registry
    /// is wired or the name is already taken. Matches C# Fabric.AddProcessor.
    public boolean addProcessor(String name, String type, Map<String, String> config,
                                List<String> requires, Map<String, List<String>> connections) {
        if (registry == null) return false;
        if (name == null || name.isEmpty() || type == null) return false;
        PipelineGraph g = graph;
        if (g.processors().containsKey(name)) return false;
        if (!registry.has(type)) return false;

        List<String> req = requires == null ? List.of() : List.copyOf(requires);
        for (String provName : req) context.registerDependent(provName, name);

        Processor proc = registry.create(type, config, context);

        Map<String, Processor> newProcessors = new LinkedHashMap<>(g.processors());
        newProcessors.put(name, proc);
        Map<String, Map<String, List<String>>> newConnections = new HashMap<>(g.connections());
        if (connections != null && !connections.isEmpty()) {
            Map<String, List<String>> copy = new LinkedHashMap<>();
            connections.forEach((k, v) -> copy.put(k, List.copyOf(v)));
            newConnections.put(name, copy);
        }
        graph = new PipelineGraph(newProcessors, newConnections, g.entryPoints());
        recordProcessorDef(name, type, config, req);
        return true;
    }

    public boolean removeProcessor(String name) {
        PipelineGraph g = graph;
        if (!g.processors().containsKey(name)) return false;
        Map<String, Processor> newProcessors = new LinkedHashMap<>(g.processors());
        newProcessors.remove(name);
        Map<String, Map<String, List<String>>> newConnections = new HashMap<>(g.connections());
        newConnections.remove(name);
        List<String> newEntries = new ArrayList<>(g.entryPoints());
        newEntries.remove(name);
        graph = new PipelineGraph(newProcessors, newConnections, newEntries);
        processorStates.remove(name);
        processorDefs.remove(name);
        return true;
    }

    public boolean enableProcessor(String name) {
        if (!graph.processors().containsKey(name)) return false;
        processorStates.put(name, ComponentState.ENABLED);
        return true;
    }

    /// Rebuild a running processor with a new config. Keeps its
    /// connections + name + lifecycle state; replaces the instance
    /// atomically. Rejects unknown processor names and unknown types.
    /// Type override is optional — when null, the current type from
    /// {@link #processorDefs} (or the config loader) is reused.
    public EditResult updateProcessorConfig(String name, String type, Map<String, String> config) {
        if (registry == null) return EditResult.fail("no registry wired");
        if (name == null || name.isEmpty()) return EditResult.fail("name must not be blank");
        PipelineGraph g = graph;
        if (!g.processors().containsKey(name)) return EditResult.fail("processor '" + name + "' not found");

        String effectiveType = type == null || type.isEmpty() ? processorType(name) : type;
        if (effectiveType == null || "unknown".equals(effectiveType)) {
            return EditResult.fail("cannot determine type for '" + name + "' — pass 'type' explicitly");
        }
        if (!registry.has(effectiveType)) {
            return EditResult.fail("unknown processor type '" + effectiveType + "'");
        }

        Map<String, String> cfg = config == null ? Map.of() : Map.copyOf(config);
        Processor rebuilt = registry.create(effectiveType, cfg, context);

        Map<String, Processor> newProcessors = new LinkedHashMap<>(g.processors());
        newProcessors.put(name, rebuilt);
        graph = new PipelineGraph(newProcessors, g.connections(), g.entryPoints());

        ProcessorDef prior = processorDefs.get(name);
        List<String> requires = prior == null ? List.of() : prior.requires();
        recordProcessorDef(name, effectiveType, cfg, requires);
        return EditResult.success();
    }

    // --- Connection-edge mutations ---

    /// Result of a single graph-edit call. {@code ok == false} means
    /// the edit was rejected (unknown processor, duplicate edge, etc.);
    /// {@code reason} carries a short human-readable explanation so the
    /// admin API can echo it.
    public record EditResult(boolean ok, String reason) {
        public static EditResult success() { return new EditResult(true, ""); }
        public static EditResult fail(String reason) { return new EditResult(false, reason); }
    }

    /// Add a single outbound connection. Rejects unknown processors on
    /// either end and duplicates of an edge that already exists. All
    /// other cases build a new graph and swap atomically.
    public EditResult addConnection(String from, String relationship, String to) {
        if (from == null || from.isEmpty())
            return EditResult.fail("from must not be blank");
        if (relationship == null || relationship.isEmpty())
            return EditResult.fail("relationship must not be blank");
        if (to == null || to.isEmpty())
            return EditResult.fail("to must not be blank");
        PipelineGraph g = graph;
        if (!g.processors().containsKey(from))
            return EditResult.fail("processor '" + from + "' not found");
        if (!g.processors().containsKey(to))
            return EditResult.fail("processor '" + to + "' not found");

        Map<String, Map<String, List<String>>> newConns = new HashMap<>(g.connections());
        Map<String, List<String>> rels = new LinkedHashMap<>(newConns.getOrDefault(from, Map.of()));
        List<String> targets = new ArrayList<>(rels.getOrDefault(relationship, List.of()));
        if (targets.contains(to)) {
            return EditResult.fail("connection '" + from + ":" + relationship + " → " + to + "' already exists");
        }
        targets.add(to);
        rels.put(relationship, List.copyOf(targets));
        newConns.put(from, rels);
        graph = new PipelineGraph(g.processors(), newConns, g.entryPoints());
        return EditResult.success();
    }

    public EditResult removeConnection(String from, String relationship, String to) {
        if (from == null || relationship == null || to == null)
            return EditResult.fail("from, relationship, and to must not be blank");
        PipelineGraph g = graph;
        Map<String, List<String>> rels = g.connections().get(from);
        if (rels == null || !rels.containsKey(relationship) || !rels.get(relationship).contains(to)) {
            return EditResult.fail("connection '" + from + ":" + relationship + " → " + to + "' not found");
        }
        Map<String, Map<String, List<String>>> newConns = new HashMap<>(g.connections());
        Map<String, List<String>> newRels = new LinkedHashMap<>(rels);
        List<String> newTargets = new ArrayList<>(newRels.get(relationship));
        newTargets.remove(to);
        if (newTargets.isEmpty()) {
            newRels.remove(relationship);
        } else {
            newRels.put(relationship, List.copyOf(newTargets));
        }
        if (newRels.isEmpty()) {
            newConns.remove(from);
        } else {
            newConns.put(from, newRels);
        }
        graph = new PipelineGraph(g.processors(), newConns, g.entryPoints());
        return EditResult.success();
    }

    /// Replace every outbound connection of a processor in a single
    /// atomic swap. {@code rels} with an empty map clears the processor's
    /// outbound connections entirely.
    public EditResult setConnections(String from, Map<String, List<String>> rels) {
        if (from == null || from.isEmpty())
            return EditResult.fail("from must not be blank");
        PipelineGraph g = graph;
        if (!g.processors().containsKey(from))
            return EditResult.fail("processor '" + from + "' not found");
        if (rels == null) rels = Map.of();

        // Validate every target exists before committing.
        for (List<String> targets : rels.values()) {
            for (String t : targets) {
                if (!g.processors().containsKey(t)) {
                    return EditResult.fail("target processor '" + t + "' not found");
                }
            }
        }

        Map<String, Map<String, List<String>>> newConns = new HashMap<>(g.connections());
        if (rels.isEmpty()) {
            newConns.remove(from);
        } else {
            Map<String, List<String>> copy = new LinkedHashMap<>();
            rels.forEach((rel, ts) -> copy.put(rel, List.copyOf(ts)));
            newConns.put(from, copy);
        }
        graph = new PipelineGraph(g.processors(), newConns, g.entryPoints());
        return EditResult.success();
    }

    /// Replace the set of entry points. Every name must already be a
    /// defined processor; otherwise the update is rejected.
    public EditResult setEntryPoints(List<String> names) {
        if (names == null) return EditResult.fail("names must not be null");
        PipelineGraph g = graph;
        for (String name : names) {
            if (!g.processors().containsKey(name)) {
                return EditResult.fail("processor '" + name + "' not found");
            }
        }
        graph = new PipelineGraph(g.processors(), g.connections(), List.copyOf(names));
        return EditResult.success();
    }

    public boolean disableProcessor(String name) {
        if (!graph.processors().containsKey(name)) return false;
        processorStates.put(name, ComponentState.DISABLED);
        return true;
    }

    public ComponentState processorState(String name) {
        if (!graph.processors().containsKey(name)) return ComponentState.DISABLED;
        return processorStates.getOrDefault(name, ComponentState.ENABLED);
    }

    public String processorType(String name) {
        ProcessorDef d = processorDefs.get(name);
        if (d != null) return d.type();
        Processor p = graph.processors().get(name);
        return p == null ? "unknown" : p.getClass().getSimpleName();
    }

    /// Config map captured when the processor was registered (via YAML
    /// load or API create). Returns an empty map if the processor was
    /// added without a recorded def — keeps callers (/api/flow, UI
    /// drawer) from having to null-check.
    public Map<String, String> processorConfig(String name) {
        ProcessorDef d = processorDefs.get(name);
        return d == null ? Map.of() : d.config();
    }

    /// Cascade-disable every processor that declared {@code requires}
    /// on the named provider. Called by {@link #disableProvider(String)}
    /// to match the C# semantics — disabling a provider takes down its
    /// consumers so they don't try to use it.
    public boolean disableProvider(String providerName) {
        zincflow.core.Provider p = context.getProvider(providerName);
        if (p == null) return false;
        for (String proc : context.getDependents(providerName)) {
            disableProcessor(proc);
        }
        p.disable(0);
        return true;
    }

    public boolean enableProvider(String providerName) {
        zincflow.core.Provider p = context.getProvider(providerName);
        if (p == null) return false;
        p.enable();
        return true;
    }

    // --- Sources ---

    public void addSource(Source source) {
        if (source == null) return;
        sources.put(source.name(), source);
        stats.metrics().onSourceRegistered(source);
    }

    public Source getSource(String name) { return sources.get(name); }

    public Map<String, Source> sources() { return Map.copyOf(sources); }

    public boolean startSource(String name) {
        Source s = sources.get(name);
        if (s == null) return false;
        if (!s.isRunning()) s.start(this::ingestFromSource);
        return true;
    }

    /// Source-side ingress. Returns {@code true} on pipeline acceptance,
    /// {@code false} on failure — sources use the return value to
    /// decide whether to mark the upstream item consumed.
    private boolean ingestFromSource(FlowFile ff) {
        try { ingest(ff); return true; }
        catch (RuntimeException ex) {
            log.warn("source ingest failed for {}: {}", ff.stringId(), ex.toString());
            return false;
        }
    }

    public boolean stopSource(String name) {
        Source s = sources.get(name);
        if (s == null) return false;
        if (s.isRunning()) s.stop();
        return true;
    }

    public record ProcessorDef(String type, Map<String, String> config, List<String> requires) { }

    /// Per-processor stats pulled from {@link Stats}, shaped the same way
    /// as the C# {@code GetProcessorStats} endpoint. One entry per
    /// processor currently in the graph.
    public Map<String, Map<String, Long>> processorStats() {
        Map<String, Long> counts = stats.processorCountsSnapshot();
        Map<String, Long> errors = stats.processorErrorsSnapshot();
        Map<String, Map<String, Long>> out = new LinkedHashMap<>();
        for (String name : graph.processors().keySet()) {
            out.put(name, Map.of(
                    "processed", counts.getOrDefault(name, 0L),
                    "errors", errors.getOrDefault(name, 0L)));
        }
        return out;
    }

    /// Swap the graph atomically. In-flight ingest calls complete against
    /// whichever reference they already loaded; subsequent ingests see
    /// the new graph. This is the hot-reload hook.
    public void swapGraph(PipelineGraph next) {
        this.graph = Objects.requireNonNull(next);
    }

    /// Diff the current graph against {@code next} and report
    /// {added, removed, updated, connectionsChanged}. A processor is
    /// "updated" when it reappears in the new graph with a different
    /// type or a different processor-def recorded; connection changes
    /// are tracked per-source. Results inform {@code /api/reload}'s
    /// response so operators can see what actually changed.
    public ReloadDiff applyReload(PipelineGraph next) {
        PipelineGraph before = this.graph;
        int added = 0, removed = 0, updated = 0, connectionsChanged = 0;

        for (String name : before.processors().keySet()) {
            if (!next.processors().containsKey(name)) removed++;
        }
        for (var entry : next.processors().entrySet()) {
            String name = entry.getKey();
            if (!before.processors().containsKey(name)) {
                added++;
                continue;
            }
            // Same name in both — decide updated vs connections-only
            if (before.processors().get(name) != entry.getValue()) {
                updated++;
            }
            Map<String, List<String>> oldConns = before.connections().getOrDefault(name, Map.of());
            Map<String, List<String>> newConns = next.connections().getOrDefault(name, Map.of());
            if (!oldConns.equals(newConns)) {
                connectionsChanged++;
            }
        }

        this.graph = Objects.requireNonNull(next);
        return new ReloadDiff(added, removed, updated, connectionsChanged);
    }

    public record ReloadDiff(int added, int removed, int updated, int connectionsChanged) {
        public int total() { return added + removed + updated + connectionsChanged; }
    }

    public Stats stats() {
        return stats;
    }

    public PipelineGraphKt graph() {
        return graph;
    }

    /// Push a FlowFile into the pipeline at each entry point. Returns
    /// when all downstream dispatches for this FlowFile have completed.
    ///
    /// Bracketed with {@code beginExecution}/{@code endExecution} so the
    /// {@code zinc_flow_active_executions} gauge reflects true
    /// in-flight work; the finally block guarantees the gauge decrements
    /// even if drain throws.
    public void ingest(FlowFile ff) {
        PipelineGraph g = graph; // single snapshot for this call
        if (g.entryPoints().isEmpty()) {
            log.warn("ingest called but pipeline has no entry points — dropping {}", ff.stringId());
            return;
        }
        Metrics m = stats.metrics();
        m.beginExecution();
        try {
            stats.recordIngested();
            // A work item = (processor name, flowfile to hand it). Stack is
            // local per ingest call so concurrent ingests don't share state.
            Deque<WorkItem> stack = new ArrayDeque<>();
            for (String entry : g.entryPoints()) {
                stack.push(new WorkItem(entry, ff));
            }
            drain(g, stack);
        } finally {
            m.endExecution();
        }
    }

    /// Inject a FlowFile at a specific processor rather than fanning out to
    /// entry points. Used by the UI's test-flowfile dialog and targeted
    /// replay scenarios. Throws {@link IllegalArgumentException} if the
    /// target isn't in the current graph.
    public void ingestAt(FlowFile ff, String target) {
        PipelineGraph g = graph;
        if (!g.processors().containsKey(target)) {
            throw new IllegalArgumentException("unknown target processor: " + target);
        }
        Metrics m = stats.metrics();
        m.beginExecution();
        try {
            stats.recordIngested();
            Deque<WorkItem> stack = new ArrayDeque<>();
            stack.push(new WorkItem(target, ff));
            drain(g, stack);
        } finally {
            m.endExecution();
        }
    }

    private void drain(PipelineGraph g, Deque<WorkItem> stack) {
        ProvenanceProvider prov = provenanceOrNoop();
        while (!stack.isEmpty()) {
            WorkItem item = stack.pop();
            FlowFile input = item.flowFile;

            if (input.hopCount() >= maxHops) {
                log.error("maxHops={} exceeded at processor={}, dropping {}",
                        maxHops, item.processor, input.stringId());
                stats.recordFailed(item.processor);
                prov.record(input.id(), ProvenanceProvider.EventType.FAILED,
                        item.processor, "maxHops exceeded");
                continue;
            }
            Processor processor = g.processors().get(item.processor);
            if (processor == null) {
                log.error("unknown processor '{}' referenced by graph — dropping {}",
                        item.processor, input.stringId());
                stats.recordFailed(item.processor);
                prov.record(input.id(), ProvenanceProvider.EventType.FAILED,
                        item.processor, "unknown processor");
                continue;
            }

            // Skip disabled processors — drop the FlowFile silently.
            if (processorStates.getOrDefault(item.processor, ComponentState.ENABLED) != ComponentState.ENABLED) {
                prov.record(input.id(), ProvenanceProvider.EventType.DROPPED,
                        item.processor, "processor disabled");
                stats.recordDropped();
                continue;
            }

            ProcessorResult result;
            try {
                result = processor.process(input);
            } catch (RuntimeException ex) {
                log.error("processor '{}' threw while handling {}: {}",
                        item.processor, input.stringId(), ex.toString(), ex);
                stats.recordFailed(item.processor);
                prov.record(input.id(), ProvenanceProvider.EventType.FAILED,
                        item.processor, ex.getMessage());
                dispatchFailure(g, stack, item.processor, input, ex.getMessage());
                continue;
            }
            stats.recordProcessed(item.processor);
            prov.record(input.id(), ProvenanceProvider.EventType.PROCESSED,
                    item.processor, "");
            dispatch(g, stack, item.processor, result);
        }
    }

    private void dispatch(PipelineGraph g, Deque<WorkItem> stack,
                          String from, ProcessorResult result) {
        switch (result) {
            case ProcessorResult.Single(var ff) ->
                fanOut(g, stack, from, Relationships.SUCCESS, List.of(ff.bumpHop()));
            case ProcessorResult.Multiple(var ffs) -> {
                for (FlowFile out : ffs) {
                    fanOut(g, stack, from, Relationships.SUCCESS, List.of(out.bumpHop()));
                }
            }
            case ProcessorResult.Routed(var route, var ff) ->
                fanOut(g, stack, from, route, List.of(ff.bumpHop()));
            case ProcessorResult.MultiRouted(var outputs) -> {
                for (var entry : outputs) {
                    fanOut(g, stack, from, entry.route(), List.of(entry.flowFile().bumpHop()));
                }
            }
            case ProcessorResult.Dropped d -> stats.recordDropped();
            case ProcessorResult.Failure(var reason, var ff) ->
                dispatchFailure(g, stack, from, ff, reason);
        }
    }

    private void dispatchFailure(PipelineGraph g, Deque<WorkItem> stack,
                                 String from, FlowFile ff, String reason) {
        List<String> failureTargets = g.next(from, Relationships.FAILURE);
        if (failureTargets.isEmpty()) {
            log.warn("failure at '{}' with no 'failure' connections — dropping {} (reason: {})",
                    from, ff.stringId(), reason);
            stats.recordDropped();
            return;
        }
        fanOut(g, stack, from, Relationships.FAILURE, List.of(ff.bumpHop()));
    }

    private void fanOut(PipelineGraph g, Deque<WorkItem> stack,
                        String from, String relationship, List<FlowFile> ffs) {
        List<String> targets = g.next(from, relationship);
        if (targets.isEmpty()) {
            return; // sink / terminal branch
        }
        for (String target : targets) {
            for (FlowFile ff : ffs) {
                stack.push(new WorkItem(target, ff));
            }
        }
    }

    private record WorkItem(String processor, FlowFile flowFile) {}
}
