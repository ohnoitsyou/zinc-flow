package zincflow.processors

import zincflow.core.ContentResolver
import zincflow.core.ContentStore
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.fabric.FlowFileV3

/** Inverse of [PackageFlowFileV3]: treat the FlowFile's content
 * as V3-framed bytes (which may hold N FlowFiles concatenated) and
 * emit each unpacked FlowFile with its original attributes restored.
 * 
 * On non-V3 input (no magic header) emits a Failure so the caller can
 * route to an error handler rather than silently discarding the blob. */
class UnpackageFlowFileV3 @JvmOverloads constructor(private val store: ContentStore? = null) : Processor {
    override fun process(ff: FlowFile): ProcessorResult {
        val resolved = ContentResolver.resolve(ff.content, store)
        if (!resolved.ok()) {
            return ProcessorResult.Failure("UnpackageFlowFileV3: " + resolved.error, ff)
        }
        val data = resolved.bytes
        if (data.size < FlowFileV3.MAGIC_LEN) {
            return ProcessorResult.Failure(
                "UnpackageFlowFileV3: payload too small for V3 magic", ff
            )
        }
        for (i in 0..<FlowFileV3.MAGIC_LEN) {
            if (data[i] != FlowFileV3.MAGIC[i]) {
                return ProcessorResult.Failure(
                    "UnpackageFlowFileV3: missing NiFiFF3 magic — not a V3-framed FlowFile", ff
                )
            }
        }
        val unpacked = FlowFileV3.unpackAll(data)
        if (unpacked.isEmpty()) {
            return ProcessorResult.Failure(
                "UnpackageFlowFileV3: V3 stream contained no FlowFiles", ff
            )
        }
        if (unpacked.size == 1) {
            return ProcessorResult.Single(unpacked[0])
        }
        return ProcessorResult.Multiple(unpacked)
    }
}
