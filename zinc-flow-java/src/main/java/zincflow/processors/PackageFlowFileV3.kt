package zincflow.processors

import zincflow.core.ContentResolver
import zincflow.core.ContentStore
import zincflow.core.FlowFile
import zincflow.core.FlowFileAttributes
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RawContent
import zincflow.core.RecordContent
import zincflow.fabric.FlowFileV3

/** Wrap a FlowFile's attributes + content into a single NiFi FlowFile V3
 * binary blob. The output FlowFile carries that blob as its raw
 * content; downstream sinks (PutFile / PutHTTP / PutStdout) write it
 * verbatim, and another V3-aware reader can round-trip the original
 * attributes.
 * 
 * Turns V3 framing into a pipeline step rather than a sink-only
 * concern — mirror of zinc-flow-csharp's PackageFlowFileV3. */
class PackageFlowFileV3 @JvmOverloads constructor(private val store: ContentStore? = null) : Processor {
    override fun process(ff: FlowFile): ProcessorResult {
        if (ff.content is RecordContent) {
            return ProcessorResult.Failure(
                "PackageFlowFileV3: RecordContent not supported — serialize to raw first", ff
            )
        }
        val resolved = ContentResolver.resolve(ff.content, store)
        if (!resolved.ok()) {
            return ProcessorResult.Failure("PackageFlowFileV3: " + resolved.error, ff)
        }
        val packed = FlowFileV3.pack(ff, resolved.bytes)
        val out = ff.withContent(RawContent(packed))
            .withAttribute(FlowFileAttributes.HTTP_CONTENT_TYPE, "application/flowfile-v3")
            .withAttribute("v3.packaged", "true")
        return ProcessorResult.Single(out)
    }
}
