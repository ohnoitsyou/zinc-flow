package zincflow.fabric

import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorContext
import zincflow.core.ProcessorResult
import zincflow.providers.LoggingProvider

class ZincConfigProcessor(
    val processorRegistry: ProcessorRegistry,
    val sourceRegistry: SourceRegistry,
    val providerRegistry: ProviderRegistry,
) {
    private val processorSpecCache = mutableMapOf<String, ProcessorSpec>()
    fun toPipeline(config: ZincConfig, ctx: ProcessorContext): PipelineGraph {
        val flow = config.flow


        // Validate each portion of the config to ensure we either already have or
        //   can create the entities we need
        // ===== Processors =====
        val p = flow.processors
        val specs = p.mapValues { (_, it) ->
            ProcessorSpec(it.type, it.config)
        }
        val processors = specs.map { (name, spec) ->
            val nameWithHash = "$name-${spec.hashCode()}"
            val s = processorSpecCache.putIfAbsent(nameWithHash, spec)
            val p = processorRegistry.

        }

        // ===== Providers =====

        // ===== Sources =====


        // DAG validation
        // -- Not sure if this should be first, depends on if it requires
        //      things to exist or not



        TODO()
    }
}

fun interface ProcessorFactory {
    fun create(name: String, config: Map<String, String>, ctx: ProcessorContext): Processor
}

class LogAttributeProcessor: ProcessorFactory {
    val logger = LoggingProvider()
    val configKeys = parameters.mapNotNull(ParamInfo::name)

    fun typeInfo() {
        ProcessorRegistry.TypeInfo(
            name = PROCESSOR_NAME,
            version = PROCESSOR_VERSION,
            description = PROCESSOR_DESCRIPTION,
            configKeys =  configKeys,
            relationships = relationships,
            category = PROCESSOR_CATEGORY,
            parameters = parameters,
        )
    }

    override fun create(name: String, config: Map<String, String>, ctx: ProcessorContext): Processor {
        return object : Processor {
            val logger = ctx.getProviderAs(LoggingProvider.NAME, LoggingProvider::class.java)
            val prefix = config[PREFIX_CONFIG_KEY]

            init {
                val missing = parameters.filter { it.required }.filterNot { config.containsKey(it.name) }
                if (missing.isNotEmpty()) {
                    throw IllegalStateException(missing.joinToString { "Parameter $it was missing from configuration" })
                }
            }

            override fun name(): String {
                return name
            }

            override fun process(ff: FlowFile): ProcessorResult {
                logger?.info(
                    "LogAttribute",
                    prefix,
                    mapOf("ff" to ff.stringId(), "size" to ff.content.size(), "attrs" to ff.attributes)
                )
                return ProcessorResult.Single(ff)
            }
        }
    }
    companion object {
        const val PREFIX_CONFIG_KEY = "prefix"
        const val PROCESSOR_NAME = "LogAttribute"
        const val PROCESSOR_VERSION = "1.0.0"
        const val PROCESSOR_DESCRIPTION = "Log FLowFile attributes and pass downstream"
        const val PROCESSOR_CATEGORY = "Attribute"
        val parameters = listOf(ParamInfo.of(PREFIX_CONFIG_KEY).description("Log line prefix").defaultValue("").build())
        val relationships = listOf("success")
    }
}

fun main() {
    val prefix = ParamInfo.of("prefix").defaultValue("").build()

}