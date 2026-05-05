package zincflow.fabric;

import zincflow.core.Processor;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Immutable description of the processor DAG. Built from config.yaml
/// (see {@link zincflow.fabric.ConfigLoader}) and passed to
/// {@link Pipeline} for execution. Swap the whole graph atomically
/// for hot reload.
///
/// @param processors  processor name → processor instance
/// @param connections fromProcessor → relationship (e.g. "success",
///                    "failure", "matched") → list of target processor names
/// @param entryPoints processor names that receive fresh FlowFiles from
///                    sources (top of the DAG)
public record PipelineGraphJ(
        Map<String, Processor> processors,
        Map<String, Map<String, List<String>>> connections,
        List<String> entryPoints
) {

    public PipelineGraphJ {
        // Preserve insertion order — Map.copyOf uses an internal
        // randomized hash, which would scramble processor declaration
        // order and break YAML round-trips through YamlEmitter.
        processors = Collections.unmodifiableMap(new LinkedHashMap<>(processors));
        Map<String, Map<String, List<String>>> conns = new LinkedHashMap<>();
        for (var entry : connections.entrySet()) {
            Map<String, List<String>> rels = new LinkedHashMap<>();
            for (var rel : entry.getValue().entrySet()) {
                rels.put(rel.getKey(), List.copyOf(rel.getValue()));
            }
            conns.put(entry.getKey(), Collections.unmodifiableMap(rels));
        }
        connections = Collections.unmodifiableMap(conns);
        entryPoints = List.copyOf(entryPoints);
    }

    public static PipelineGraphJ empty() {
        return new PipelineGraphJ(Map.of(), Map.of(), List.of());
    }

    /// Next processor names reachable from {@code fromProcessor} along
    /// the given {@code relationship}. Empty list if none — the executor
    /// treats that as "terminal for this branch."
    public List<String> next(String fromProcessor, String relationship) {
        Map<String, List<String>> outgoing = connections.get(fromProcessor);
        if (outgoing == null) return List.of();
        List<String> targets = outgoing.get(relationship);
        return targets == null ? List.of() : targets;
    }
}
