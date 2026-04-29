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
import java.util.List

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
class RouteRecord(spec: String?) : Processor {
    @JvmRecord
    private data class Route(val name: String?, val predicate: JexlExpression?)

    private val routes: MutableList<Route>

    init {
        val parsed: MutableList<Route?> = ArrayList<Route?>()
        if (spec != null) {
            val entries: Array<String?> = spec.split(";".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            for (i in entries.indices) {
                val entry = entries[i]!!.trim { it <= ' ' }
                if (entry.isEmpty()) continue
                val colon = entry.indexOf(':')
                require(colon > 0) {
                    ("RouteRecord: malformed route at index " + i + ": '" + entry
                            + "' — expected 'name: expression'")
                }
                val name = entry.substring(0, colon).trim { it <= ' ' }
                val exprStr = entry.substring(colon + 1).trim { it <= ' ' }
                require(!name.isEmpty()) { "RouteRecord: route at index " + i + " has empty name" }
                require(!exprStr.isEmpty()) { "RouteRecord: route '" + name + "' has empty expression" }
                require("unmatched" != name) { "RouteRecord: 'unmatched' is reserved for records that match no route" }
                try {
                    parsed.add(Route(name, JEXL.createExpression(exprStr)))
                } catch (ex: JexlException) {
                    throw IllegalArgumentException(
                        ("RouteRecord: route '" + name + "' has invalid expression '"
                                + exprStr + "': " + ex.message), ex
                    )
                }
            }
        }
        this.routes = List.copyOf<Route?>(parsed)
    }

    override fun process(ff: FlowFile): ProcessorResult? {
        if (ff.content !is RecordContent) {
            return ProcessorResult.single(ff)
        }

        // Partition in insertion order so downstream emission order is
        // deterministic across runs.
        val buckets: MutableMap<String?, MutableList<MutableMap<String?, Any?>?>?> =
            LinkedHashMap<String?, MutableList<MutableMap<String?, Any?>?>?>()
        for (record in rc.records) {
            var matched: String? = null
            for (route in routes) {
                val ctx = MapContext()
                ctx.set("record", record)
                for (e in record.entries) ctx.set(e.key, e.value)
                val v: Any?
                try {
                    v = route.predicate!!.evaluate(ctx)
                } // A record missing a referenced field becomes null under safe
                // JEXL; the compare then returns null. Either way we treat
                // the record as not matching this route, not as a whole-
                // FlowFile failure.
                catch (ex: JexlException) {
                    continue
                }
                if (isTruthy(v)) {
                    matched = route.name
                    break
                }
            }
            val key = if (matched == null) "unmatched" else matched
            buckets.computeIfAbsent(key) { `_`: kotlin.String? -> java.util.ArrayList<kotlin.collections.MutableMap<kotlin.String?, kotlin.Any?>?>() }!!
                .add(record)
        }

        if (buckets.isEmpty()) return ProcessorResult.dropped()

        val entries: MutableList<MultiRouted.Entry?> = ArrayList<MultiRouted.Entry?>(buckets.size)
        for (e in buckets.entries) {
            val child = RecordContent(e.value, rc.schema)
            entries.add(MultiRouted.Entry(e.key, ff.withContent(child)))
        }
        return ProcessorResult.multiRouted(entries)
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
