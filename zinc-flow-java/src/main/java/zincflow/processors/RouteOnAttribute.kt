package zincflow.processors

import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.Relationships
import java.util.Locale
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
class RouteOnAttribute(spec: String) : Processor {
    private val rules: List<Rule> = parse(spec)

    override fun process(ff: FlowFile): ProcessorResult {
        for (rule in rules) {
            if (rule.evaluate(ff.attributes)) {
                return ProcessorResult.Routed(rule.name, ff)
            }
        }
        return ProcessorResult.Routed(Relationships.UNMATCHED, ff)
    }

    enum class Op(vararg val tokens: String) {
        EQ("==", "EQ"),
        NEQ("!=", "NEQ"),
        CONTAINS("CONTAINS"),
        STARTSWITH("STARTSWITH"),
        ENDSWITH("ENDSWITH"),
        MATCHES("MATCHES"),
        EXISTS("EXISTS"),
        GT(">", "GT"),
        GE(">=", "GE"),
        LT("<", "LT"),
        LE("<=", "LE");


        companion object {
            val tokenMap = entries.flatMap { e -> e.tokens.map { it to e } }.toMap()

            fun parse(token: String, routeName: String): Op {
                return tokenMap[token.uppercase(Locale.getDefault())] ?: throw IllegalArgumentException(
                    "RouteOnAttribute: route '$routeName' has unsupported operator '$token' — valid: ${tokenMap.keys}"
                )
            }
        }
    }

    @JvmRecord
    data class Rule(val name: String, val attribute: String, val op: Op, val value: String) {
        fun evaluate(attributes: Map<String, String>): Boolean {
            if (op == Op.EXISTS) {
                return attributes.containsKey(attribute)
            }
            val actual = attributes[attribute] ?: return false
            return when (op) {
                Op.EQ -> (value == actual)
                Op.NEQ -> (value != actual)
                Op.CONTAINS -> actual.contains(value)
                Op.STARTSWITH -> actual.startsWith(value)
                Op.ENDSWITH -> actual.endsWith(value)
                Op.MATCHES -> actual.matches(value.toRegex())
                Op.GT, Op.GE, Op.LT, Op.LE -> compareTo(actual, value, op)
            }
        }

        companion object {
            /** Numeric comparison when both sides parse as doubles, lexicographic
             * fallback otherwise. Matches the zinc-flow-csharp semantics: string
             * compare when values aren't parseable as numbers, so GT/LT can be
             * used on version strings, timestamps-as-ISO-strings, etc. */
            private fun compareTo(actual: String, expected: String, op: Op?): Boolean {
                val cmp: Int = try {
                    val a = actual.toDouble()
                    val b = expected.toDouble()
                    a.compareTo(b)
                } catch (_: NumberFormatException) {
                    actual.compareTo(expected)
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
        private fun parse(spec: String): List<Rule> {
            if (spec.isBlank()) return listOf()

            val out = mutableListOf<Rule>()
            val entries = spec.split(";").mapNotNull { e -> e.trim().takeIf { it.isNotEmpty() } }
            for ((i, entry) in entries.withIndex()) {
                require(entry.contains(":")) {
                    "RouteOnAttribute: malformed route at index $i: '$entry' — expected format: '<name>: <attr OP value>'"
                }

                val (routeName, condition) = entry.split(":", limit = 1)
                // EXISTS takes no value: "routeA: attr EXISTS" is valid.
                val parts = condition.split("\\s+".toRegex(), limit = 3)
                require(parts.size >= 2) {
                    "RouteOnAttribute: route '$routeName' has malformed condition: '$condition' — expected 'attr OP [value]'"
                }

                val op: Op = Op.parse(parts[1], routeName)
                val value = if (parts.size >= 3) parts[2] else ""
                require(!(op != Op.EXISTS && parts.size < 3)) {
                    ("RouteOnAttribute: route '" + routeName + "' operator " + op
                            + " requires a value but none was provided")
                }

                out.add(Rule(routeName, parts[0], op, value))
            }
            return out
        }
    }
}
