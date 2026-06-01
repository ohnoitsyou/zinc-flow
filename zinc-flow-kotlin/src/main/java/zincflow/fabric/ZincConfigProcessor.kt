package zincflow.fabric

import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorContext
import zincflow.core.ProcessorResult
import zincflow.core.Provider
import zincflow.core.Source
import zincflow.providers.LoggingProvider

class ZincConfigProcessor(
    val processorRegistry: ProcessorRegistry,
    val sourceRegistry: SourceRegistry,
    val providerRegistry: ProviderRegistry,
) {
    private val processorSpecCache = mutableMapOf<String, ProcessorSpec>()
    private var lastProcessors = listOf<Processor>()
    private var lastSources = listOf<Source>()
    private var lastProviders = listOf<Provider>()



    fun toPipeline(appConfig: SlimZincConfig, ctx: ProcessorContext): PipelineGraph {
        val flow = appConfig.zincConfig.flow

        // Validate each portion of the config to ensure we either already have or
        //   can create the entities we need
        // ===== Processors =====
        val processors = flow.processors.entries.asSequence().mapNotNull { (name, config) ->
            val spec = ProcessorSpec(config.type, config.config)
            val nameWithHash = "$name-${spec.hashCode()}"
            processorSpecCache.putIfAbsent(nameWithHash, spec)
            val processor = processorRegistry.create(spec.type, spec.config, ctx)
            if (processor != null) name to processor else null
        }.toMap()
        lastProcessors = processors.values.toList()

        // ===== Connections =====
        val connections = flow.connections.mapValues { (_, it) -> it.connections }

        // ===== Providers =====
        val providers = flow.providers.mapNotNull { (_, it) ->
            providerRegistry.create(it.type, it.config)
        }
        lastProviders = providers

        // ===== Sources =====
        val sources = flow.sources.mapNotNull { (name, config) ->
            sourceRegistry.create(config.type, name, config.config)
        }
        lastSources = sources

        // ===== Entry Points =====
        val entryPoints = flow.entryPoints

        // DAG validation
        // -- Not sure if this should be first, depends on if it requires
        //      things to exist or not
        return PipelineGraph(processors, connections, entryPoints, sources)
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
        const val PROCESSOR_DESCRIPTION = "Log FlowFile attributes and pass downstream"
        const val PROCESSOR_CATEGORY = "Attribute"
        val parameters = listOf(ParamInfo.of(PREFIX_CONFIG_KEY).description("Log line prefix").defaultValue("").build())
        val relationships = listOf("success")
    }
}

fun main() {
    val prefix = ParamInfo.of("prefix").defaultValue("").build()
}