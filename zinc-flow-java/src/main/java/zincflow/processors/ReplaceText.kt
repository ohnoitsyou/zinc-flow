package zincflow.processors

import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RawContent
import java.nio.charset.StandardCharsets
import java.util.regex.Matcher
import java.util.regex.Pattern

/** Regex search-and-replace on the FlowFile's text payload. The match
 * pattern is treated as a Java regex; the replacement supports back
 * references like `$1` per [Matcher.replaceAll].
 * 
 * Config mirrors zinc-flow-csharp's `ReplaceText`
 * (StdLib/TextProcessors.cs:11-40):
 * pattern     — required regex
 * replacement — default ""
 * mode        — "all" (default) or "first" */
class ReplaceText @JvmOverloads constructor(pattern: String, replacement: String?, mode: String? = "all") : Processor {
    private val pattern: Pattern
    private val replacement: String
    private val firstOnly: Boolean

    init {
        requireNotNull(pattern) { "ReplaceText: pattern must not be null" }
        this.pattern = Pattern.compile(pattern)
        this.replacement = if (replacement == null) "" else replacement
        this.firstOnly = "first".equals(mode, ignoreCase = true)
    }

    override fun process(ff: FlowFile): ProcessorResult {
        val content = ff.content
        if (content !is RawContent) {
            return ProcessorResult.failure(
                "ReplaceText: expected RawContent, got " + content.javaClass.getSimpleName(), ff
            )
        }
        val input = String(content.bytes, StandardCharsets.UTF_8)
        val matcher = pattern.matcher(input)
        val output = if (firstOnly) matcher.replaceFirst(replacement) else matcher.replaceAll(replacement)
        return ProcessorResult.single(ff.withContent(RawContent(output.toByteArray(StandardCharsets.UTF_8))))
    }
}
