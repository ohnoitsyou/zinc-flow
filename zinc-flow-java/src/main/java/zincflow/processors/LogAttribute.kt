package zincflow.processors

import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.providers.LoggingProvider

/** Logs a FlowFile (id, size, attributes) under the configured prefix,
 * then passes it through on the "success" connection unchanged. Always
 * goes through a [LoggingProvider]; when the pipeline wires a
 * shared provider the operator can flip it off at runtime to mute
 * chatty LogAttribute instances. */
class LogAttribute @JvmOverloads constructor(prefix: String? = "", logger: LoggingProvider? = null) : Processor {
    private val prefix: String = prefix ?: ""
    private val logger: LoggingProvider = logger ?: LoggingProvider().apply { enable() }

    override fun process(ff: FlowFile): ProcessorResult {
        logger.info(
            "LogAttribute",
            prefix,
            mapOf(
                "ff" to ff.stringId(),
                "size" to ff.content.size(),
                "attrs" to ff.attributes,
            )
        )
        return ProcessorResult.Single(ff)
    }
}
