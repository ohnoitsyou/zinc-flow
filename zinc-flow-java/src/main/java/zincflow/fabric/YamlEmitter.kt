package zincflow.fabric

import org.yaml.snakeyaml.DumperOptions
import org.yaml.snakeyaml.Yaml

/** Serialises a running [PipelineGraph] + recorded
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
    @JvmStatic
    fun emit(graph: PipelineGraph, specs: Map<String, ProcessorSpec>): String? {
        val flow = mutableMapOf<String, Any>()
        flow["entryPoints"] = graph.entryPoints.toList()

        // Processors — preserve declaration order from the current graph.
        val processors = mutableMapOf<String, Any>()
        for (name in graph.processors.keys) {
            val spec = specs[name]
            val entry = mutableMapOf<String, Any>()
            entry[ConfigLoader.TYPE_KEY] = spec?.type ?: graph.processors[name]?.javaClass?.getSimpleName() ?: "UNKNOWN"
            if (spec != null && !spec.config.isEmpty()) {
                entry[ConfigLoader.CONFIG_KEY] = spec.config.toMap()
            }
            processors[name] = entry
        }
        flow["processors"] = processors

        // Connections — only include processors with non-empty outbound.
        if (graph.connections.isNotEmpty()) {
            val connections = mutableMapOf<String, Any>()
            for (entry in graph.connections.entries) {
                val rels = mutableMapOf<String, Any>()
                for (rel in entry.value.entries) {
                    rels[rel.key] = rel.value.toList()
                }
                if (rels.isNotEmpty()) connections[entry.key] = rels
            }
            if (connections.isNotEmpty()) flow["connections"] = connections
        }

        val top = mutableMapOf<String, Any>()
        top["flow"] = flow

        return dumper().dump(top)
    }

    private fun dumper(): Yaml {
        val opts = DumperOptions()
        opts.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
        opts.setIndent(2)
        opts.setIndicatorIndent(0)
        opts.isPrettyFlow = true
        return Yaml(opts)
    }
}
