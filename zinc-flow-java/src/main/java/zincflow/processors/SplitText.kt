package zincflow.processors

import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RawContent
import java.nio.charset.StandardCharsets
import java.util.Map
import java.util.regex.Pattern
import kotlin.math.max

/** Splits the FlowFile's text payload into multiple FlowFiles — one per
 * non-empty split segment. Mirrors zinc-flow-csharp's `SplitText`
 * (StdLib/TextProcessors.cs:100-160):
 * delimiter   — always treated as a regex
 * headerLines — number of leading lines to prepend to every chunk
 * (for CSV-style header replication); 0 disables
 * 
 * Emitted FlowFiles carry `split.index` and `split.count`
 * attributes; the original attributes are NOT inherited (matches C#).
 * When the delimiter doesn't match, the input FlowFile passes through
 * unchanged. */
class SplitText @JvmOverloads constructor(delimiter: String, headerLines: Int = 0) : Processor {
    private val delimiter: Pattern
    private val headerLines: Int

    init {
        require(!(delimiter == null || delimiter.isEmpty())) { "SplitText: delimiter must not be blank" }
        this.delimiter = Pattern.compile(delimiter)
        this.headerLines = max(0, headerLines)
    }

    override fun process(ff: FlowFile): ProcessorResult {
        val content = ff.content
        if (content !is RawContent) {
            return ProcessorResult.failure(
                "SplitText: expected RawContent, got " + content.javaClass.getSimpleName(), ff
            )
        }
        val text = String(content.bytes, StandardCharsets.UTF_8)
        var parts = delimiter.split(text, -1)

        if (parts.size <= 1) {
            return ProcessorResult.single(ff)
        }

        var header = ""
        if (headerLines > 0) {
            val lines: Array<String?> = text.split("\n".toRegex()).toTypedArray()
            if (lines.size > headerLines) {
                val hb = StringBuilder()
                for (i in 0..<headerLines) hb.append(lines[i]).append('\n')
                header = hb.toString()
                val rem = StringBuilder()
                for (i in headerLines..<lines.size) {
                    rem.append(lines[i])
                    if (i < lines.size - 1) rem.append('\n')
                }
                parts = delimiter.split(rem.toString(), -1)
            }
        }

        val out: MutableList<FlowFile?> = ArrayList<FlowFile?>(parts.size)
        for (i in parts.indices) {
            if (parts[i].isBlank()) continue
            val chunk = header + parts[i]
            val piece = FlowFile.create(
                chunk.toByteArray(StandardCharsets.UTF_8),
                Map.of<String?, String?>(
                    "split.index", i.toString(),
                    "split.count", parts.size.toString()
                )
            )
            out.add(piece)
        }

        if (out.isEmpty()) {
            return ProcessorResult.single(ff)
        }
        return ProcessorResult.multiple(out)
    }
}
