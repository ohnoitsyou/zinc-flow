package zincflow.fabric

import zincflow.core.Processor
import zincflow.core.Source
import java.util.Collections
import kotlin.collections.mapValues
import kotlin.collections.toMap

/** Immutable description of the processor DAG. Built from config.yaml

 * (see [ConfigLoader]) and passed to
 * [Pipeline] for execution. Swap the whole graph atomically
 * for hot reload.
 * 
 * @param processors  processor name → processor instance
 * @param connections fromProcessor → relationship (e.g. "success",
 * "failure", "matched") → list of target processor names
 * @param entryPoints processor names that receive fresh FlowFiles from
 * sources (top of the DAG)
 */

data class PipelineGraph @JvmOverloads constructor(
    val processors: Map<String, Processor>,
    val connections: Map<String, Map<String, List<String>>>,
    val entryPoints: List<String>,
    val sources: List<Source>,
    val graphVersion: Int = 0,
) {
    companion object {
        fun empty() = PipelineGraph(emptyMap(), emptyMap(), emptyList(), emptyList())
        fun of(
            processors: Map<String, Processor>,
            connections: Map<String, Map<String, List<String>>>,
            entryPoints: List<String>,
            sources: List<Source>,
            graphVersion: Int = 0
        ): PipelineGraph {
            return PipelineGraph(
                processors = processors.toMap(),
                connections = connections.toMap(),
//                    Collections.unmodifiableMap(
//                    connections.mapValues { (_, rels) ->
//                        Collections.unmodifiableMap(
//                            rels.mapValues { (_, targets) ->
//                                java.util.List.copyOf(targets)
//                            }.toMap()
//                        )
//                    }.toMap()
//                ),
                entryPoints = entryPoints.toList(),
                sources = sources.toList(),
                graphVersion = graphVersion + 1
            )
        }
    }

    fun next(fromProcessor: String, relationship: String): List<String> {
        return connections[fromProcessor]?.get(relationship) ?: emptyList()
    }
}
