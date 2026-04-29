package zincflow.processors

import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.Relationships
import java.lang.Double
import java.util.List
import kotlin.Array
import kotlin.Boolean
import kotlin.IllegalArgumentException
import kotlin.Int
import kotlin.NumberFormatException
import kotlin.String
import kotlin.require

/** Routes a FlowFile to a named relationship based on the first matching
 * rule in a rules-spec string. Rules form:
 * `"routeA: attr OP value; routeB: attr OP value"`.
 * 
 * Supported operators (mirrors zinc-flow-csharp):
 * 
 *  * `==` / `EQ`, `!=` / `NEQ` — equality
 *  * `CONTAINS`, `STARTSWITH`, `ENDSWITH` — substring tests
 *  * `MATCHES` — full-string regex
 *  * `EXISTS` — attribute presence (value ignored)
 *  * `GT`, `GE`, `LT`, `LE` — numeric comparison
 * (both sides parsed as doubles; fall back to lexicographic for non-numeric values)
 * 
 * 
 * No rule matches → routes to "unmatched". Downstream connections
 * decide what happens from there; wire a terminal processor or an
 * UpdateAttribute to "unmatched" to handle the fall-through explicitly. */
class RouteOnAttribute(spec: String?) : Processor {
    private val rules: MutableList<Rule>

    init {
        this.rules = parse(spec)
    }

    override fun process(ff: FlowFile): ProcessorResult {
        for (rule in rules) {
            if (rule.evaluate(ff.attributes)) {
                return ProcessorResult.routed(rule.name, ff)
            }
        }
        return ProcessorResult.routed(Relationships.UNMATCHED, ff)
    }

    fun rules(): MutableList<Rule> {
        return rules
    }

    enum class Op {
        EQ, NEQ, CONTAINS, STARTSWITH, ENDSWITH, MATCHES, EXISTS, GT, GE, LT, LE;

        companion object {
            fun parse(token: String, routeName: String?): Op {
                return when (token.uppercase()) {
                    "==", "EQ" -> Op.EQ
                    "!=", "NEQ" -> Op.NEQ
                    "CONTAINS" -> Op.CONTAINS
                    "STARTSWITH" -> Op.STARTSWITH
                    "ENDSWITH" -> Op.ENDSWITH
                    "MATCHES" -> Op.MATCHES
                    "EXISTS" -> Op.EXISTS
                    ">", "GT" -> Op.GT
                    ">=", "GE" -> Op.GE
                    "<", "LT" -> Op.LT
                    "<=", "LE" -> Op.LE
                    else -> throw IllegalArgumentException(
                        ("RouteOnAttribute: route '" + routeName + "' has unsupported operator '"
                                + token + "' — valid: EQ/==, NEQ/!=, CONTAINS, STARTSWITH, ENDSWITH, MATCHES, EXISTS, GT/>, GE/>=, LT/<, LE/<=")
                    )
                }
            }
        }
    }

    @JvmRecord
    data class Rule(val name: String?, val attribute: String?, val op: Op?, val value: String?) {
        fun evaluate(attributes: MutableMap<String?, String?>): Boolean {
            if (op == Op.EXISTS) {
                return attributes.containsKey(attribute)
            }
            val actual = attributes.get(attribute)
            if (actual == null) return false
            return when (op) {
                Op.EQ -> (value == actual)
                Op.NEQ -> (value != actual)
                Op.CONTAINS -> actual.contains(value!!)
                Op.STARTSWITH -> actual.startsWith(value!!)
                Op.ENDSWITH -> actual.endsWith(value!!)
                Op.MATCHES -> actual.matches(value!!.toRegex())
                Op.GT, Op.GE, Op.LT, Op.LE -> Companion.compareTo(actual, value!!, op)
                Op.EXISTS -> true
            }
        }

        companion object {
            /** Numeric comparison when both sides parse as doubles, lexicographic
             * fallback otherwise. Matches the zinc-flow-csharp semantics: string
             * compare when values aren't parseable as numbers, so GT/LT can be
             * used on version strings, timestamps-as-ISO-strings, etc. */
            private fun compareTo(actual: String, expected: String, op: Op?): Boolean {
                var cmp: Int
                try {
                    val a = actual.toDouble()
                    val b = expected.toDouble()
                    cmp = Double.compare(a, b)
                } catch (ignored: NumberFormatException) {
                    cmp = actual.compareTo(expected)
                }
                return when (op) {
                    Op.GT -> cmp > 0
                    Op.GE -> cmp >= 0
                    Op.LT -> cmp < 0
                    Op.LE -> cmp <= 0
                    else -> false
                }
            }
        }
    }

    companion object {
        // --- Parsing ---
        private fun parse(spec: String?): MutableList<Rule> {
            if (spec == null || spec.isBlank()) return mutableListOf<Rule?>()
            val out: MutableList<Rule?> = ArrayList<Rule?>()
            val entries: Array<String?> = spec.split(";".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            for (i in entries.indices) {
                val entry = entries[i]!!.trim { it <= ' ' }
                if (entry.isEmpty()) continue
                val colonIdx = entry.indexOf(':')
                require(colonIdx > 0) {
                    ("RouteOnAttribute: malformed route at index " + i + ": '" + entry
                            + "' — expected 'name: attr OP value'")
                }
                val routeName = entry.substring(0, colonIdx).trim { it <= ' ' }
                val condition = entry.substring(colonIdx + 1).trim { it <= ' ' }
                // EXISTS takes no value: "routeA: attr EXISTS" is valid.
                val parts: Array<String?> = condition.split("\\s+".toRegex(), limit = 3).toTypedArray()
                require(parts.size >= 2) {
                    ("RouteOnAttribute: route '" + routeName + "' has malformed condition: '"
                            + condition + "' — expected 'attr OP [value]'")
                }
                val op: Op = Op.Companion.parse(parts[1]!!, routeName)
                val value = if (parts.size >= 3) parts[2] else ""
                require(!(op != Op.EXISTS && parts.size < 3)) {
                    ("RouteOnAttribute: route '" + routeName + "' operator " + op
                            + " requires a value but none was provided")
                }
                out.add(Rule(routeName, parts[0], op, value))
            }
            return List.copyOf<Rule?>(out)
        }
    }
}
