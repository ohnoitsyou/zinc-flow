package zincflow.processors

import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.providers.LoggingProvider
import java.util.Map
import java.util.Objects
import java.util.function.Supplier

/** Logs a FlowFile (id, size, attributes) under the configured prefix,
 * then passes it through on the "success" connection unchanged. Always
 * goes through a [LoggingProvider]; when the pipeline wires a
 * shared provider the operator can flip it off at runtime to mute
 * chatty LogAttribute instances. */
class LogAttribute @JvmOverloads constructor(prefix: String?, logger: LoggingProvider? = null) : Processor {
    private val prefix: String
    private val logger: LoggingProvider

    init {
        this.prefix = if (prefix == null) "" else prefix
        this.logger = Objects.requireNonNullElseGet<LoggingProvider>(
            logger,
            Supplier { obj: LoggingProvider? -> LoggingProvider.enabled() })
    }

    override fun process(ff: FlowFile): ProcessorResult {
        logger.info(
            "LogAttribute",
            prefix,
            Map.of<String?, Any?>(
                "ff", ff.stringId(),
                "size", ff.content.size(),
                "attrs", ff.attributes
            )
        )
        return ProcessorResult.single(ff)
    }
}
