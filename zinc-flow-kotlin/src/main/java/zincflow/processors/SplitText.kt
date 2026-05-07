package zincflow.processors

import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RawContent
import java.nio.charset.StandardCharsets
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
    private val delimiter: Pattern = delimiter.takeIf { it.isNotBlank() }
        ?.let { Pattern.compile(it) }
            ?: throw IllegalArgumentException("SplitText: delimiter must not be blank")
    private val headerLines: Int = headerLines.coerceAtLeast(0)

    override fun process(ff: FlowFile): ProcessorResult {
        val content = ff.content
        if (content !is RawContent) {
            return ProcessorResult.Failure(
                "SplitText: expected RawContent, got " + content.javaClass.getSimpleName(), ff
            )
        }
        val text = String(content.bytes, StandardCharsets.UTF_8)
        var parts = delimiter.split(text, -1)

        if (parts.size <= 1) {
            return ProcessorResult.Single(ff)
        }

        var header = ""
        if (headerLines > 0) {
            val lines: Array<String?> = text.split("\n".toRegex()).toTypedArray()
            if (lines.size > headerLines) {
                header = buildString {
                    for (i in 0..<headerLines) append(lines[i]).append('\n')
                }
                val rem = buildString {
                    for (i in headerLines..<lines.size) {
                        append(lines[i])
                        if (i <= lines.lastIndex) append('\n')
                    }
                }
                parts = delimiter.split(rem, -1)
            }
        }

        val out = parts.withIndex().mapNotNull { (idx, part) ->
            part.takeIf { it.isNotBlank() }?.let {
                val chunk = header + part
                FlowFile.create(
                    chunk.toByteArray(StandardCharsets.UTF_8),
                    mapOf(
                        "split.index" to "$idx",
                        "split.count" to "${parts.size}"
                    )
                )
            }
        }
        if (out.isEmpty()) {
            return ProcessorResult.Single(ff)
        }
        return ProcessorResult.Multiple(out)
    }
}
