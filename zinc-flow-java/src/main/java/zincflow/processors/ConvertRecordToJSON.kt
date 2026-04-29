package zincflow.processors

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RawContent
import zincflow.core.RecordContent
import java.nio.charset.StandardCharsets

/** Serializes a [RecordContent] payload back to JSON bytes in a
 * [RawContent]. The wrapping shape is controlled by
 * `singleObject`: when false (default), emits a JSON array; when
 * true, emits the first record as a bare object (useful when a pipeline
 * fans back to HTTP APIs that don't accept arrays). */
class ConvertRecordToJSON @JvmOverloads constructor(private val singleObject: Boolean = false) : Processor {
    override fun process(ff: FlowFile): ProcessorResult {
        if (ff.content !is RecordContent) {
            return ProcessorResult.failure(
                "ConvertRecordToJSON: expected RecordContent, got " + ff.content.javaClass.getSimpleName(), ff
            )
        }
        try {
            val bytes: ByteArray?
            if (singleObject) {
                if (rc.records.isEmpty()) {
                    bytes = "{}".toByteArray(StandardCharsets.UTF_8)
                } else {
                    bytes = MAPPER.writeValueAsBytes(rc.records.getFirst())
                }
            } else {
                bytes = MAPPER.writeValueAsBytes(rc.records)
            }
            return ProcessorResult.single(ff.withContent(RawContent(bytes)))
        } catch (ex: JsonProcessingException) {
            return ProcessorResult.failure("ConvertRecordToJSON: serialize failed — " + ex.message, ff)
        }
    }

    companion object {
        private val MAPPER = ObjectMapper()
    }
}
