package zincflow.processors

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import zincflow.core.ClaimContent
import zincflow.core.ContentResolver
import zincflow.core.ContentStore
import zincflow.core.FlowFile
import zincflow.core.FlowFileAttributes
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.core.RawContent
import zincflow.fabric.FlowFileV3
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/** Writes the FlowFile payload to a file under `directory`. The
 * file name defaults to the FlowFile's string id; the `filename`
 * attribute overrides when present.
 * 
 * `format=v3` packs the attributes + content into a NiFi
 * FlowFile V3 blob before writing — lossless round-trip through
 * another V3-aware reader. Claim-backed content resolves through the
 * supplied [ContentStore]. */
class PutFile @JvmOverloads constructor(
    directory: String,
    private val append: Boolean = false,
    format: String? = "raw",
    private val store: ContentStore? = null
) : Processor {
    private val directory: Path
    private val v3: Boolean = "v3".equals(format, ignoreCase = true)

    init {
        require(directory.isNotEmpty()) { "PutFile: directory must not be blank" }
        this.directory = Path.of(directory)
    }

    override fun process(ff: FlowFile): ProcessorResult {
        val name = ff.attributes.getOrDefault(FlowFileAttributes.FILENAME, ff.stringId())
        val target = directory.resolve(name)
        try {
            Files.createDirectories(directory)

            // V3 framing inlines the attribute header + byte body, so we
            // always resolve to a single buffer for that path. The
            // non-V3 path streams ClaimContent directly so multi-GB
            // payloads never get loaded into the heap.
            if (v3) {
                val resolved = ContentResolver.resolve(ff.content, store)
                if (!resolved.ok()) return fail(ff, resolved.error)
                val packed = FlowFileV3.pack(ff, resolved.bytes)
                writeBytes(target, packed)
            } else if (ff.content is ClaimContent && store != null) {
                store.openRead(ff.content.claimId).use { `in` ->
                    Files.newOutputStream(target, *openOptions()).use { out ->
                        `in`.transferTo(out)
                    }
                }
            } else if (ff.content is RawContent) {
                writeBytes(target, ff.content.bytes)
            } else {
                val resolved = ContentResolver.resolve(ff.content, store)
                if (!resolved.ok()) return fail(ff, resolved.error)
                writeBytes(target, resolved.bytes)
            }
            return ProcessorResult.Single(ff.withAttribute("putfile.path", target.toAbsolutePath().toString()))
        } catch (ex: IOException) {
            log.error("PutFile: write failed for $target: $ex")
            return ProcessorResult.Failure(ex.message ?: "", ff)
        }
    }

    private fun fail(ff: FlowFile, reason: String?): ProcessorResult {
        log.error("PutFile: $reason")
        return ProcessorResult.Failure("PutFile: $reason", ff)
    }

    @Throws(IOException::class)
    private fun writeBytes(target: Path, bytes: ByteArray) {
        Files.write(target, bytes, *openOptions())
    }

    private fun openOptions(): Array<StandardOpenOption> {
        return buildList {
            add(StandardOpenOption.CREATE)
            add(if (append) StandardOpenOption.APPEND else StandardOpenOption.TRUNCATE_EXISTING)
        }.toTypedArray()
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(PutFile::class.java)
    }
}
