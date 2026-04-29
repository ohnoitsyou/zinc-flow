package zincflow.processors

import org.apache.commons.jexl3.JexlBuilder
import org.apache.commons.jexl3.JexlEngine
import org.apache.commons.jexl3.JexlException
import org.apache.commons.jexl3.JexlExpression
import org.apache.commons.jexl3.MapContext
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RecordContent
import java.util.Map

/** Evaluates one or more Apache Commons JEXL 3 expressions against the
 * FlowFile's attributes (and the first record, when the payload is
 * RecordContent). Each expression produces a FlowFile attribute named
 * by the entry's target.
 * 
 * Mirrors zinc-flow-csharp's `EvaluateExpression` shape
 * (StdLib/ExpressionProcessors.cs:23) — multi-output, target→expression.
 * The expression ENGINE differs: Java uses JEXL (full arithmetic,
 * booleans, ternary, lambdas); C# uses a string-template DSL with a
 * fixed function set. Configs are therefore not yet interoperable;
 * C# is slated to gain arithmetic + booleans in the post-Java cohort.
 * 
 * Variables visible in each expression:
 * 
 *  * `attributes` — `Map<String,String>` of FlowFile attributes
 *  * `record`     — first record as a `Map<String,Object>`
 * (null when payload is not RecordContent)
 *  * `records`    — full list of records (same caveat)
 *  * `id`         — FlowFile id as a long
 *  * `contentSize` — Content size (bytes or record count). Named
 * `contentSize` not `size` to avoid
 * JEXL's reserved `size` operator.
 */
class EvaluateExpression(expressionsByTarget: MutableMap<String, String>) : Processor {
    private val expressions: MutableMap<String?, JexlExpression?>

    init {
        require(!(expressionsByTarget == null || expressionsByTarget.isEmpty())) { "EvaluateExpression: expressions map must have at least one target=expression entry" }
        val compiled: MutableMap<String?, JexlExpression?> = LinkedHashMap<String?, JexlExpression?>()
        for (entry in expressionsByTarget.entries) {
            val target = entry.key
            val source = entry.value
            require(!(target == null || target.isBlank())) { "EvaluateExpression: target attribute must not be blank" }
            require(!(source == null || source.isBlank())) { "EvaluateExpression: expression for '" + target + "' must not be blank" }
            try {
                compiled.put(target, ENGINE.createExpression(source))
            } catch (ex: JexlException) {
                throw IllegalArgumentException(
                    "EvaluateExpression: invalid JEXL for '" + target + "' — " + ex.message, ex
                )
            }
        }
        this.expressions = Map.copyOf<String?, JexlExpression?>(compiled)
    }

    override fun process(ff: FlowFile): ProcessorResult {
        val ctx = MapContext()
        ctx.set("attributes", ff.attributes)
        ctx.set("id", ff.id)
        ctx.set("contentSize", ff.content.size())
        if (ff.content is RecordContent) {
            val records: MutableList<MutableMap<String?, Any?>?> = rc.records
            ctx.set("records", records)
            ctx.set("record", if (records.isEmpty()) null else records.getFirst())
        } else {
            ctx.set("records", mutableListOf<Any?>())
            ctx.set("record", null)
        }
        var result = ff
        for (entry in expressions.entries) {
            try {
                val value = entry.value!!.evaluate(ctx)
                val asString: String? = if (value == null) "" else value.toString()
                result = result.withAttribute(entry.key, asString)
            } catch (ex: JexlException) {
                return ProcessorResult.failure(
                    "EvaluateExpression: evaluation failed for '" + entry.key + "' — " + ex.message, ff
                )
            }
        }
        return ProcessorResult.single(result)
    }

    companion object {
        private val ENGINE: JexlEngine = JexlBuilder()
            .strict(false) // Undefined vars evaluate to null rather than throwing.
            .safe(true)
            .silent(false) // Still surface syntax errors.
            .create()
    }
}
