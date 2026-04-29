package zincflow.processors

import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult

/** Sets (or replaces) a single attribute on the FlowFile and forwards
 * on "success". Multi-attribute updates are a Phase 3 follow-up. */
class UpdateAttribute(key: String, value: String?) : Processor {
    private val key: String
    private val value: String

    init {
        require(!(key == null || key.isEmpty())) { "UpdateAttribute: key must not be blank" }
        this.key = key
        this.value = if (value == null) "" else value
    }

    override fun process(ff: FlowFile): ProcessorResult {
        return ProcessorResult.single(ff.withAttribute(key, value))
    }
}
