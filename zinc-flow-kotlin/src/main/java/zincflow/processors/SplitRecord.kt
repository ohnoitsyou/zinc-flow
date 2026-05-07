package zincflow.processors

import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RecordContent

/** Fan out a RecordContent FlowFile into one FlowFile per record. Each
 * output carries the original attributes plus zero-padded `split.index`
 * and `split.total` attributes so downstream sorting by index is
 * lexically stable.
 * 
 * This is the dataflow analogue of list unfolding — a stream of records
 * becomes a stream of single-record events that can then be routed,
 * transformed, or sunk independently. Pairs naturally with
 * [RouteRecord] for per-record routing. */
class SplitRecord : Processor {
    override fun process(ff: FlowFile): ProcessorResult {
        val content = ff.content
        if (content !is RecordContent || content.records.isEmpty()) {
            return ProcessorResult.Single(ff)
        }

        val total: Int = content.records.size
        val width = total.toString().length
        val totalStr = total.toString()

        val children = mutableListOf<FlowFile>()
        for (i in 0..<total) {
            val record: Map<String, Any> = content.records[i]
            val child = RecordContent(listOf(record), content.schema)
            val emitted = ff
                .withContent(child)
                .withAttribute("split.index", padLeft(i.toString(), width))
                .withAttribute("split.total", totalStr)
            children.add(emitted)
        }
        return ProcessorResult.Multiple(children)
    }

    companion object {
        private fun padLeft(s: String, width: Int): String {
            if (s.length >= width) return s
            return buildString {
                repeat(width - s.length) { append('0') }
                append(s)
            }
        }
    }
}
