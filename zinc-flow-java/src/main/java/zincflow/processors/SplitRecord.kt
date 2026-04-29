package zincflow.processors

import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RecordContent
import java.util.List

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
        if (ff.content !is RecordContent || rc.records.isEmpty()) {
            return ProcessorResult.single(ff)
        }

        val total: Int = rc.records.size
        val width = total.toString().length
        val totalStr = total.toString()

        val children: MutableList<FlowFile?> = ArrayList<FlowFile?>(total)
        for (i in 0..<total) {
            val record: MutableMap<String?, Any?> = rc.records.get(i)
            val child = RecordContent(List.of<MutableMap<String?, Any?>?>(record), rc.schema)
            val emitted = ff
                .withContent(child)
                .withAttribute("split.index", padLeft(i.toString(), width))
                .withAttribute("split.total", totalStr)
            children.add(emitted)
        }
        return ProcessorResult.multiple(children)
    }

    companion object {
        private fun padLeft(s: String, width: Int): String {
            if (s.length >= width) return s
            val sb = StringBuilder(width)
            for (i in 0..<width - s.length) sb.append('0')
            sb.append(s)
            return sb.toString()
        }
    }
}
