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
    private val compiled: JsonPath = jsonPathQuery.takeIf { it.isNotBlank() }?.let {
        JsonPath.compile(it)
    } ?: throw IllegalArgumentException("QueryRecord: invalid JsonPath — Must not be blank")

    override fun process(ff: FlowFile): ProcessorResult {
        val content = ff.content
        if (content !is RecordContent) {
            return ProcessorResult.Failure("QueryRecord: expected RecordContent, got ${content.javaClass.getSimpleName()}", ff)
        }
        try {
            val result: Any = compiled.read(content.records, CONFIG)
            val matches: List<Map<String, Any>> = normalise(result)
            if (matches.isEmpty()) {
                return ProcessorResult.Routed(Relationships.UNMATCHED, ff)
            }
            return ProcessorResult.Routed(
                Relationships.MATCHED,
                ff.withContent(RecordContent(matches, content.schema))
            )
        } catch (_: PathNotFoundException) {
            // With SUPPRESS_EXCEPTIONS this usually becomes an empty result,
            // but some malformed paths still throw — treat as no match.
            return ProcessorResult.Routed(Relationships.UNMATCHED, ff)
        } catch (ex: RuntimeException) {
            return ProcessorResult.Failure("QueryRecord: query failed — " + ex.message, ff)
        }
    }

    companion object {
        // Default-safe JsonPath config: suppress exceptions on missing paths
        // so a mismatched record just doesn't match rather than blowing up.
        private val CONFIG: Configuration = Configuration.defaultConfiguration()
            .addOptions(Option.SUPPRESS_EXCEPTIONS)


        private fun normalise(jsonPathResult: Any?): List<Map<String, Any>> {
            return when (jsonPathResult) {
                null -> emptyList()
                is Map<*, *> -> listOf(jsonPathResult.entries.associate {(key, value) ->  key.toString() to value as Any })
                is List<*> -> {
                    buildList {
                        for (item in jsonPathResult) {
                            if (item is Map<*, *>) {
                                add(item.entries.associate { (k, v) -> k.toString() to v as Any })
                            }
                        }
                    }
                }
                else -> emptyList()
            }
        }
    }
}
