package zincflow.processors

import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult

/** Sets (or replaces) a single attribute on the FlowFile and forwards
 * on "success". Multi-attribute updates are a Phase 3 follow-up. */
class UpdateAttribute(private val key: String, value: String?) : Processor {
    private val value: String = value.takeUnless { it == null } ?: ""

    init {
        require(key.isNotBlank()) { "UpdateAttribute: key must not be blank" }
    }

    override fun process(ff: FlowFile): ProcessorResult {
        return ProcessorResult.Single(ff.withAttribute(key, value))
    }
}
