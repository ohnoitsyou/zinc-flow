package zincflow.fabric

import org.yaml.snakeyaml.DumperOptions
import org.yaml.snakeyaml.Yaml
import zincflow.fabric.ConfigLoader.ProcessorSpec
import java.util.List
import java.util.Map

/** Serialises a running [PipelineGraphKt] + recorded
 * [ProcessorSpec] map back to YAML. Used by
 * `POST /api/flow/save` so the UI can commit graph edits
 * (processors added / removed / reconfigured through the admin
 * API) back to disk without the operator hand-editing YAML.
 * 
 * Uses SnakeYAML's [Yaml.dump] with block style and
 * a single-space indent, which is the idiomatic shape for
 * zinc-flow config files. Output keys are ordered
 * `flow → entryPoints, processors, connections` so
 * round-tripped files stay diff-stable against the committed base. */
object YamlEmitter {
    /** Emit a complete YAML document for the current graph + specs. */
    fun emit(graph: PipelineGraphKt, specs: MutableMap<String?, ProcessorSpec?>?): String? {
        var specs = specs
        requireNotNull(graph) { "graph must not be null" }
        if (specs == null) specs = Map.of<String?, ProcessorSpec?>()

        val flow: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        flow.put("entryPoints", List.copyOf<String?>(graph.entryPoints))

        // Processors — preserve declaration order from the current graph.
        val procs: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        for (name in graph.processors.keySet()) {
            val spec = specs.get(name)
            val entry: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
            entry.put(
                ConfigLoader.Companion.TYPE_KEY, if (spec == null)
                    graph.processors.get(name).getClass().getSimpleName()
                else
                    spec.type
            )
            if (spec != null && !spec.config.isEmpty()) {
                entry.put(ConfigLoader.Companion.CONFIG_KEY, LinkedHashMap<String?, String?>(spec.config))
            }
            procs.put(name, entry)
        }
        flow.put("processors", procs)

        // Connections — only include processors with non-empty outbound.
        if (!graph.connections.isEmpty()) {
            val connections: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
            for (entry in graph.connections.entrySet()) {
                val rels: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
                for (rel in entry.getValue().entrySet()) {
                    rels.put(rel.getKey(), List.copyOf<String?>(rel.getValue()))
                }
                if (!rels.isEmpty()) connections.put(entry.getKey(), rels)
            }
            if (!connections.isEmpty()) flow.put("connections", connections)
        }

        val top: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        top.put("flow", flow)

        return dumper().dump(top)
    }

    private fun dumper(): Yaml {
        val opts = DumperOptions()
        opts.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
        opts.setIndent(2)
        opts.setIndicatorIndent(0)
        opts.setPrettyFlow(true)
        return Yaml(opts)
    }
}
