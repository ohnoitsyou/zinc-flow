package zincflow.fabric

import java.util.ArrayDeque

/** Static DAG check over a processor graph. Three classes of finding:
 * 
 *  * **errors** — unknown connection targets. Load must abort.
 *  * **warnings** — cycles, unreachable processors, no entry
 * points. Non-fatal; surfaced to the operator.
 *  * **entry points** — processors with no inbound connections,
 * so the executor knows where to inject ingested FlowFiles.
 * 
 * 
 * Port of zinc-flow-csharp's DagValidator — called from
 * [ConfigLoader] after processors and connections are built so
 * a misconfigured config surfaces every problem in one go rather than
 * the first-one-wins style of the previous reference-existence check. */
object FlowValidator {
    /** Validate the processor DAG. `processorNames` is the full
     * set of defined processors (so the unreachable check can warn
     * on sink processors declared but never wired); `connections`
     * maps `fromProcessor → relationship → [target,...]`. */
    fun validate(
        processorNames: MutableCollection<String>,
        connections: MutableMap<String, MutableMap<String, MutableList<String>>>
    ): Result {
        val errors = mutableListOf<String>()
        val warnings = mutableListOf<String>()
        val allProcessors = processorNames.toMutableSet()

        // Build adjacency (flatten relationships) + collect referenced targets.
        val adjacency = mutableMapOf<String, MutableList<String>>()
        val referenced = mutableSetOf<String>()
        for (processor in allProcessors) {
            val relationships = connections.getOrDefault(processor, mutableMapOf())
            val targets = mutableListOf<String>()
            for (destinations in relationships.values) {
                for (dest in destinations) {
                    targets.add(dest)
                    referenced.add(dest)
                    if (!allProcessors.contains(dest)) {
                        errors.add("processor '$processor' connects to unknown target '$dest'")
                    }
                }
            }
            adjacency[processor] = targets
        }

        // Entry points: processors nobody targets.
        val entryPoints = allProcessors.filterNot { processor -> referenced.contains(processor) }
        if (entryPoints.isEmpty() && !allProcessors.isEmpty()) {
            warnings.add("no entry-point processors detected (every processor is also a target)")
        }

        // Cycle detection (DFS 3-color).
        val color = allProcessors.associateWith { 0 }.toMutableMap()
        for (processor in allProcessors) {
            if (color[processor] == 0) {
                detectCycles(processor, adjacency, color, mutableListOf(), warnings, allProcessors)
            }
        }

        // Unreachable processors: BFS from entry points.
        if (entryPoints.isNotEmpty()) {
            val reachable = mutableSetOf<String>()
            val queue = ArrayDeque<String>(entryPoints)
            while (queue.isNotEmpty()) {
                val current = queue.poll()
                if (!reachable.add(current)) continue
                for (t in adjacency.getOrDefault(current, emptyList())) {
                    if (allProcessors.contains(t) && !reachable.contains(t)) queue.add(t)
                }
            }
            for (proc in allProcessors) {
                if (!reachable.contains(proc)) {
                    warnings.add("processor '$proc' is not reachable from any entry point")
                }
            }
        }

        return Result(errors, warnings, entryPoints)
    }

    private fun detectCycles(
        node: String,
        adjacency: Map<String, List<String>>,
        color: MutableMap<String, Int>,
        path: MutableList<String>,
        warnings: MutableList<String>,
        all: Set<String>
    ) {
        color[node] = 1
        path.add(node)
        for (target in adjacency.getOrDefault(node, mutableListOf())) {
            if (!all.contains(target)) continue
            val c = color[target] ?: continue
            if (c == 1) {
                // gray = back edge = cycle
                val cycleStart = path.indexOf(target)
                val warning = buildString {
                    append("cycle detected: ")
                    for (i in cycleStart..<path.size) {
                        append(path[i]).append(" → ")
                    }
                    append(target)
                }
                warnings.add(warning)
            } else if (c == 0) {
                detectCycles(target, adjacency, color, path, warnings, all)
            }
        }
        path.removeAt(path.lastIndex)
        color[node] = 2
    }

    @ConsistentCopyVisibility
    data class Result private constructor(val errors: List<String>, val warnings: List<String>, val entryPoints: List<String>) {
        companion object {
            operator fun invoke(errors: List<String>, warnings: List<String>, entryPoints: List<String>): Result {
                return Result(errors.toList(), warnings.toList(), entryPoints.toList())
            }
        }

        fun ok() = errors.isEmpty()
    }
}
