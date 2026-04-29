package zincflow.fabric

import java.util.ArrayDeque
import java.util.Deque
import java.util.List
import java.util.Map

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
        processorNames: MutableCollection<String?>,
        connections: MutableMap<String?, MutableMap<String?, MutableList<String?>>>
    ): Result {
        val errors: MutableList<String?> = ArrayList<String?>()
        val warnings: MutableList<String?> = ArrayList<String?>()
        val all: MutableSet<String?> = LinkedHashSet<String?>(processorNames)

        // Build adjacency (flatten relationships) + collect referenced targets.
        val adjacency: MutableMap<String?, MutableList<String?>?> = HashMap<String?, MutableList<String?>?>()
        val referenced: MutableSet<String?> = HashSet<String?>()
        for (proc in all) {
            val rels = connections.getOrDefault(proc, Map.of<String?, MutableList<String?>?>())
            val targets: MutableList<String?> = ArrayList<String?>()
            for (dests in rels.values) {
                for (dest in dests) {
                    targets.add(dest)
                    referenced.add(dest)
                    if (!all.contains(dest)) {
                        errors.add("processor '" + proc + "' connects to unknown target '" + dest + "'")
                    }
                }
            }
            adjacency.put(proc, targets)
        }

        // Entry points: processors nobody targets.
        val entryPoints: MutableList<String?> = ArrayList<String?>()
        for (proc in all) if (!referenced.contains(proc)) entryPoints.add(proc)
        if (entryPoints.isEmpty() && !all.isEmpty()) {
            warnings.add("no entry-point processors detected (every processor is also a target)")
        }

        // Cycle detection (DFS 3-color).
        val color: MutableMap<String?, Int?> = HashMap<String?, Int?>()
        for (proc in all) color.put(proc, 0)
        val path: MutableList<String?> = ArrayList<String?>()
        for (proc in all) {
            if (color.get(proc) == 0) {
                detectCycles(proc, adjacency, color, path, warnings, all)
            }
        }

        // Unreachable processors: BFS from entry points.
        if (!entryPoints.isEmpty()) {
            val reachable: MutableSet<String?> = HashSet<String?>()
            val queue: Deque<String?> = ArrayDeque<String?>(entryPoints)
            while (!queue.isEmpty()) {
                val current = queue.poll()
                if (!reachable.add(current)) continue
                for (t in adjacency.getOrDefault(current, mutableListOf<String?>())!!) {
                    if (all.contains(t) && !reachable.contains(t)) queue.add(t)
                }
            }
            for (proc in all) {
                if (!reachable.contains(proc)) {
                    warnings.add("processor '" + proc + "' is not reachable from any entry point")
                }
            }
        }

        return Result(errors, warnings, entryPoints)
    }

    private fun detectCycles(
        node: String?,
        adjacency: MutableMap<String?, MutableList<String?>?>,
        color: MutableMap<String?, Int?>,
        path: MutableList<String?>,
        warnings: MutableList<String?>,
        all: MutableSet<String?>
    ) {
        color.put(node, 1)
        path.add(node)
        for (target in adjacency.getOrDefault(node, mutableListOf<String?>())!!) {
            if (!all.contains(target)) continue
            val c = color.get(target)
            if (c == null) continue
            if (c == 1) {
                // gray = back edge = cycle
                val cycleStart = path.indexOf(target)
                val sb = StringBuilder("cycle detected: ")
                for (i in cycleStart..<path.size) sb.append(path.get(i)).append(" → ")
                sb.append(target)
                warnings.add(sb.toString())
            } else if (c == 0) {
                detectCycles(target, adjacency, color, path, warnings, all)
            }
        }
        path.removeAt(path.size - 1)
        color.put(node, 2)
    }

    class Result(errors: MutableList<String?>?, warnings: MutableList<String?>?, entryPoints: MutableList<String?>?) {
        fun ok(): Boolean {
            return errors!!.isEmpty()
        }

        val errors: MutableList<String?>?
        val warnings: MutableList<String?>?
        val entryPoints: MutableList<String?>?

        init {
            var errors = errors
            var warnings = warnings
            var entryPoints = entryPoints
            errors = List.copyOf<String?>(errors)
            warnings = List.copyOf<String?>(warnings)
            entryPoints = List.copyOf<String?>(entryPoints)
            this.errors = errors
            this.warnings = warnings
            this.entryPoints = entryPoints
        }
    }
}
