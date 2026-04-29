package zincflow.processors

import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Option
import com.jayway.jsonpath.PathNotFoundException
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RecordContent
import zincflow.core.Relationships
import java.util.List

/** Filters RecordContent by a JsonPath predicate (Jayway JsonPath). The
 * query is applied to the full record list, and the matching subset
 * becomes a new RecordContent. Routing:
 * 
 * 
 *  * non-empty matches  → `matched` relationship
 *  * empty matches      → `unmatched` (FlowFile passes through untouched)
 * 
 * 
 * Example query (records where `priority == "high"`):
 * `$[?(@.priority == 'high')]` */
class QueryRecord(jsonPathQuery: String) : Processor {
    private val compiled: JsonPath

    init {
        require(!(jsonPathQuery == null || jsonPathQuery.isEmpty())) { "QueryRecord: jsonPath query must not be blank" }
        try {
            this.compiled = JsonPath.compile(jsonPathQuery)
        } catch (ex: RuntimeException) {
            throw IllegalArgumentException(
                "QueryRecord: invalid JsonPath — " + ex.message, ex
            )
        }
    }

    override fun process(ff: FlowFile): ProcessorResult {
        if (ff.content !is RecordContent) {
            return ProcessorResult.failure(
                "QueryRecord: expected RecordContent, got " + ff.content.javaClass.getSimpleName(), ff
            )
        }
        try {
            val result: Any? = compiled.read<Any?>(rc.records, CONFIG)
            val matches: MutableList<MutableMap<String?, Any?>?> = normalise(result)
            if (matches.isEmpty()) {
                return ProcessorResult.routed(Relationships.UNMATCHED, ff)
            }
            return ProcessorResult.routed(
                Relationships.MATCHED,
                ff.withContent(RecordContent(matches, rc.schema))
            )
        } catch (ex: PathNotFoundException) {
            // With SUPPRESS_EXCEPTIONS this usually becomes an empty result,
            // but some malformed paths still throw — treat as no match.
            return ProcessorResult.routed(Relationships.UNMATCHED, ff)
        } catch (ex: RuntimeException) {
            return ProcessorResult.failure("QueryRecord: query failed — " + ex.message, ff)
        }
    }

    companion object {
        // Default-safe JsonPath config: suppress exceptions on missing paths
        // so a mismatched record just doesn't match rather than blowing up.
        private val CONFIG: Configuration? = Configuration.defaultConfiguration()
            .addOptions(Option.SUPPRESS_EXCEPTIONS)

        private fun normalise(jsonPathResult: Any?): MutableList<MutableMap<String?, Any?>?> {
            if (jsonPathResult == null) return mutableListOf<MutableMap<String?, Any?>?>()
            if (jsonPathResult is MutableList<*>) {
                val out: MutableList<MutableMap<String?, Any?>?> =
                    ArrayList<MutableMap<String?, Any?>?>(jsonPathResult.size)
                for (item in jsonPathResult) {
                    if (item is MutableMap<*, *>) out.add(item as MutableMap<String?, Any?>)
                }
                return out
            }
            if (jsonPathResult is MutableMap<*, *>) {
                return List.of<MutableMap<String?, Any?>?>(jsonPathResult as MutableMap<String?, Any?>)
            }
            return mutableListOf<MutableMap<String?, Any?>?>()
        }
    }
}
