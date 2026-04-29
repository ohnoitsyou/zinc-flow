package zincflow.processors

import zincflow.core.ContentResolver
import zincflow.core.ContentStore
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RecordContent
import java.nio.charset.StandardCharsets
import java.util.List
import java.util.regex.Matcher
import java.util.regex.Pattern

/** Regex capture-group extractor: run `pattern` against the FlowFile's
 * raw content, pull out named and positional capture groups, and write each
 * group into a FlowFile attribute. Pure pass-through when the pattern
 * doesn't match — the original FlowFile flows on "success" unchanged.
 * 
 * Named groups (`(?<city>...)`) become attributes named after the
 * group. Positional groups are mapped to attribute names via the
 * `groupNames` config (comma-separated, positional — first name
 * maps to group 1, second to group 2, and so on). Empty names in that
 * list skip that group.
 * 
 * Mirrors zinc-flow-csharp's ExtractText (StdLib/TextProcessors.cs). */
class ExtractText(regex: String, groupNames: String?, store: ContentStore?) : Processor {
    private val pattern: Pattern
    private val positionalGroupNames: MutableList<String>
    private val store: ContentStore?

    override fun process(ff: FlowFile): ProcessorResult {
        if (ff.content is RecordContent) {
            return ProcessorResult.failure(
                "ExtractText: RecordContent not supported — serialize to raw first",
                ff
            )
        }

        val resolved = ContentResolver.resolve(ff.content, store)
        if (!resolved.ok()) {
            return ProcessorResult.failure("ExtractText: " + resolved.error, ff)
        }

        val text = String(resolved.bytes, StandardCharsets.UTF_8)
        val matcher = pattern.matcher(text)
        if (!matcher.find()) {
            return ProcessorResult.single(ff)
        }

        var out = ff

        // Named groups — detect and lift into attributes.
        for (name in namedGroups(pattern)) {
            val value = matcher.group(name)
            if (value != null) {
                out = out.withAttribute(name, value)
            }
        }

        // Positional groups — mapped via the groupNames config.
        val available = matcher.groupCount()
        var i = 0
        while (i < positionalGroupNames.size && i < available) {
            val attr = positionalGroupNames.get(i)
            if (attr.isEmpty()) {
                i++
                continue
            }
            val value = matcher.group(i + 1)
            if (value != null) {
                out = out.withAttribute(attr, value)
            }
            i++
        }

        return ProcessorResult.single(out)
    }

    init {
        require(!(regex == null || regex.isEmpty())) { "ExtractText: pattern must not be blank" }
        this.pattern = Pattern.compile(regex)
        this.positionalGroupNames = parseGroupNames(groupNames)
        this.store = store
    }

    companion object {
        private fun parseGroupNames(spec: String?): MutableList<String> {
            if (spec == null || spec.isBlank()) return mutableListOf<String?>()
            val parts = spec.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            val out: MutableList<String?> = ArrayList<String?>(parts.size)
            for (p in parts) out.add(p.trim { it <= ' ' })
            return List.copyOf<String?>(out)
        }

        /** Discover named groups by scraping the pattern text — Java's
         * [Pattern] doesn't expose them directly. Handles the
         * `(?<name>...)` syntax, which is the only named-group form
         * [Pattern.compile] accepts. */
        private fun namedGroups(p: Pattern): MutableSet<String> {
            val names: MutableSet<String> = LinkedHashSet<String>()
            val m: Matcher = NAMED_GROUP_REGEX.matcher(p.pattern())
            while (m.find()) names.add(m.group(1))
            return names
        }

        private val NAMED_GROUP_REGEX: Pattern = Pattern.compile("\\(\\?<([A-Za-z][A-Za-z0-9]*)>")
    }
}
