package zincflow.processors

import org.apache.commons.jexl3.JexlBuilder
import org.apache.commons.jexl3.JexlEngine
import org.apache.commons.jexl3.JexlException
import org.apache.commons.jexl3.JexlExpression
import org.apache.commons.jexl3.MapContext
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.ProcessorResult.MultiRouted
import zincflow.core.RecordContent
import java.util.ArrayList

/** Partition each incoming RecordContent's records across named routes
 * based on per-route expression predicates evaluated against the record's
 * fields. First matching route wins; records matching no route are
 * emitted to `unmatched`.
 * 
 * Mirrors zinc-flow-csharp's `RouteRecord`
 * (StdLib/ExpressionProcessors.cs) — the record-level counterpart to
 * `RouteOnAttribute`. Config key `routes` is a semicolon-
 * delimited list of `name: expression` pairs. Each expression
 * evaluates via Apache Commons JEXL with every record field exposed as
 * a top-level variable plus the full map available as `record`. */
class RouteRecord(spec: String) : Processor {
    @JvmRecord
    private data class Route(val name: String, val predicate: JexlExpression)

    private val routes: List<Route> = spec.parseToRoutes()

    private fun String.parseToRoutes(): List<Route> {
        val parsed = mutableListOf<Route>()
        val entries: List<String> = this.split(";").mapNotNull { e -> e.trim().takeIf { it.isNotEmpty() } }
        for ((i, entry) in entries.withIndex()) {
            require(entry.contains(":")) {
                "RouteRecord: malformed route at index $i: '$entry' — expected 'name: expression'"
            }

            val (name, expression) = entry.split(":", limit = 1)
            require(name.isNotBlank()) { "RouteRecord: route at index $i has empty name" }
            require(expression.isNotBlank()) { "RouteRecord: route '$name' has empty expression" }
            require("unmatched" != name) { "RouteRecord: 'unmatched' is reserved for records that match no route" }

            try {
                parsed.add(Route(name, JEXL.createExpression(expression)))
            } catch (ex: JexlException) {
                throw IllegalArgumentException(
                    ("RouteRecord: route '" + name + "' has invalid expression '"
                            + expression + "': " + ex.message), ex
                )
            }
        }
        return parsed
    }

    override fun process(ff: FlowFile): ProcessorResult {
        val content = ff.content
        if (content !is RecordContent) {
            return ProcessorResult.Single(ff)
        }

        // Partition in insertion order so downstream emission order is
        // deterministic across runs.
        val buckets = mutableMapOf<String, MutableList<Map<String, Any>>>()
        for (record in content.records) {
            val key = routes.firstOrNull { route ->
                val ctx = MapContext().apply {
                    set("record", record)
                    record.entries.forEach {
                        set(it.key, it.value)
                    }
                }
                try {
                    isTruthy(route.predicate.evaluate(ctx))
                } catch (_: JexlException) {
                    // A record missing a referenced field becomes null under safe
                    // JEXL; the compare then returns null. Either way we treat
                    // the record as not matching this route, not as a whole-
                    // FlowFile failure.
                    false
                }
            }
                ?.name ?: "unmatched"

            buckets.computeIfAbsent(key) { mutableListOf() }.add(record)
        }

        if (buckets.isEmpty()) return ProcessorResult.Dropped()

        return MultiRouted(buckets.entries.map { (key, value) ->
            ProcessorResult.Routed(key, ff.withContent(RecordContent(value, content.schema)))
        })
    }

    companion object {
        private val JEXL: JexlEngine = JexlBuilder()
            .strict(false).safe(true).silent(false).create()

        private fun isTruthy(v: Any?): Boolean {
            if (v == null) return false
            if (v is Boolean) return v
            if (v is Number) return v.toDouble() != 0.0
            if (v is String) return !v.isEmpty()
            return true
        }
    }
}
