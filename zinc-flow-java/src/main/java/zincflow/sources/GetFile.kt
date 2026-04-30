package zincflow.sources

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import zincflow.core.FlowFile
import zincflow.core.FlowFileAttributes
import zincflow.core.PollingSource
import zincflow.core.Source
import zincflow.core.SourcePlugin
import zincflow.fabric.FlowFileV3
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.List
import java.util.Map
import java.util.concurrent.ConcurrentHashMap

/** Poll a directory for files and stream them into the pipeline. Files
 * consumed by the pipeline are moved to a `.processed/`
 * subdirectory so the next poll doesn't re-emit them.
 * 
 * <h2>V3 unpacking</h2>
 * When `unpackV3` is true (default) and a scanned file begins
 * with the `NiFiFF3` magic, the source unpacks every FlowFile
 * in the blob and emits them individually. Attributes from the packed
 * FlowFiles are preserved; the source layers on top:
 * `filename`, `path`, `source`,
 * `v3.frame.index`, `v3.frame.count`.
 * Regular files get `filename`, `path`, `size`, `source`.
 * 
 * Config (under `sources.file`):
 * <pre>
 * sources:
 * file:
 * inputDir: /var/spool/zincflow
 * pattern: "*"
 * pollIntervalMs: 1000
 * unpackV3: true
</pre> * 
 * 
 * Mirrors zinc-flow-csharp's GetFile. */
class GetFile(
    name: String,
    val inputDir: Path,
    pattern: String?,
    pollIntervalMillis: Long,
    val unpackV3: Boolean,
    override val isRunning: Boolean
) : PollingSource(name, pollIntervalMillis) {
    private val pattern: String = if (pattern.isNullOrEmpty()) "*" else pattern
    val processedDir: Path = inputDir.resolve(PROCESSED_DIR)

    // filename-per-FlowFile so onIngested can move the right source
    // file. We can't key on attrs because V3 unpacking emits N FlowFiles
    // per file and we only move the underlying file once all N land.
    private val pendingMoves = mutableMapOf<Long, Path>()
    private val outstandingPerFile = mutableMapOf<Path, Int>()

    override fun sourceType(): String = TYPE

    public override fun poll(): MutableList<FlowFile> {
        ensureDirs()
        val out = mutableListOf<FlowFile>()
        try {
            Files.newDirectoryStream(inputDir, pattern).use { stream ->
                for (entry in stream) {
                    if (Files.isDirectory(entry)) continue
                    val emitted = emitFor(entry)
                    if (!emitted.isEmpty()) {
                        outstandingPerFile[entry] = emitted.size
                        for (ff in emitted) pendingMoves[ff.id] = entry
                        out.addAll(emitted)
                    }
                }
            }
        } catch (ex: IOException) {
            log.warn("GetFile {}: directory scan failed — {}", name(), ex.toString())
        }
        return out
    }

    private fun emitFor(file: Path): MutableList<FlowFile> {
        val bytes: ByteArray = try {
            Files.readAllBytes(file)
        } catch (ex: IOException) {
            // File might have been removed or partially written between
            // the listing and the read — skip this round, the next poll
            // picks it up.
            log.debug("GetFile {}: could not read {} ({}) — skipping", name(), file, ex.toString())
            return mutableListOf()
        }

        if (unpackV3 && looksLikeV3(bytes)) {
            val frames = FlowFileV3.unpackAll(bytes)
            if (!frames.isEmpty()) {
                val out: MutableList<FlowFile> = ArrayList<FlowFile>(frames.size)
                for (i in frames.indices) {
                    out.add(addAttrs(frames[i]!!, file, bytes.size.toLong(), true, i, frames.size))
                }
                return out
            }
            // Well-formed magic but no frames decoded — fall through and
            // treat the file as raw. Losing malformed V3 silently would
            // hide bugs; treating it as raw surfaces the bytes for ops.
        }

        return mutableListOf(
            addAttrs(
                FlowFile.create(bytes, mutableMapOf()),
                file,
                bytes.size.toLong(),
                v3 = false,
                frameIndex = 0,
                frameCount = 1
            )
        )
    }

    private fun addAttrs(
        base: FlowFile, file: Path, rawSize: Long,
        v3: Boolean, frameIndex: Int, frameCount: Int
    ): FlowFile {
        val attrs: MutableMap<String?, String?> = LinkedHashMap<String?, String?>(base.attributes)
        attrs.put(FlowFileAttributes.FILENAME, file.getFileName().toString())
        attrs.put(FlowFileAttributes.PATH, file.toAbsolutePath().toString())
        attrs.put(FlowFileAttributes.SOURCE, name())
        if (v3) {
            attrs.put(FlowFileAttributes.V3_FRAME_INDEX, frameIndex.toString())
            attrs.put(FlowFileAttributes.V3_FRAME_COUNT, frameCount.toString())
        } else {
            attrs.put(FlowFileAttributes.SIZE, rawSize.toString())
        }
        return FlowFile(base.id, attrs, base.content, base.timestampMillis, base.hopCount)
    }

    override fun onIngested(ff: FlowFile) {
        val file = pendingMoves.remove(ff.id)
        if (file == null) return
        val remaining = outstandingPerFile.computeIfPresent(file) { k: Path?, v: Int? -> v!! - 1 }
        if (remaining != null && remaining <= 0) {
            outstandingPerFile.remove(file)
            moveToProcessed(file)
        }
    }

    override fun onRejected(ff: FlowFile) {
        // Drop bookkeeping without moving the file — it stays in
        // inputDir for the next poll to retry.
        val file = pendingMoves.remove(ff.id)
        if (file != null) outstandingPerFile.remove(file)
        super.onRejected(ff)
    }

    private fun moveToProcessed(file: Path) {
        try {
            Files.createDirectories(processedDir)
            val target = processedDir.resolve(file.getFileName())
            Files.move(file, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        } catch (ex: IOException) {
            // Non-atomic fallback — some filesystems (e.g. across mount
            // boundaries) don't support ATOMIC_MOVE. Best-effort copy;
            // a failure here means the file will re-ingest on the next
            // poll, which is safer than dropping it.
            try {
                Files.createDirectories(processedDir)
                Files.move(
                    file, processedDir.resolve(file.getFileName()),
                    StandardCopyOption.REPLACE_EXISTING
                )
            } catch (ex2: IOException) {
                log.warn("GetFile {}: failed to move {} to {} — {}", name(), file, processedDir, ex2.toString())
            }
        }
    }

    private fun ensureDirs() {
        try {
            Files.createDirectories(inputDir)
        } catch (ex: IOException) { /* tolerate — poll will surface issues */
        }
    }

    /** SPI entry for ServiceLoader discovery. */
    class Plugin : SourcePlugin {
        override fun sourceType(): String {
            return TYPE
        }

        override fun description(): String {
            return "Polls a directory; emits one FlowFile per file (V3 bundles are unpacked)."
        }

        override fun configKeys(): MutableList<String?> {
            return mutableListOf<String?>("inputDir", "pattern", "pollIntervalMs", "unpackV3")
        }

        override fun create(name: String?, config: MutableMap<String?, Any?>): Source? {
            val inputDir: String = str(config.get("inputDir"))
            if (inputDir.isEmpty()) return null // disabled when inputDir is absent

            return GetFile(
                name,
                Path.of(inputDir),
                str(config.getOrDefault("pattern", "*")),
                longOr(config.get("pollIntervalMs"), 1000),
                boolOr(config.get("unpackV3"), true)
            )
        }

        companion object {
            private fun str(o: Any?): String {
                return if (o == null) "" else o.toString()
            }

            private fun longOr(o: Any?, fallback: Long): Long {
                if (o == null) return fallback
                if (o is Number) return o.toLong()
                try {
                    return o.toString().trim { it <= ' ' }.toLong()
                } catch (ex: NumberFormatException) {
                    return fallback
                }
            }

            private fun boolOr(o: Any?, fallback: Boolean): Boolean {
                if (o == null) return fallback
                if (o is Boolean) return o
                return "true".equals(o.toString().trim { it <= ' ' }, ignoreCase = true)
            }
        }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(GetFile::class.java)

        const val NAME: String = "file"
        const val TYPE: String = "GetFile"
        const val PROCESSED_DIR: String = ".processed"

        private fun looksLikeV3(data: ByteArray): Boolean {
            if (data.size < FlowFileV3.MAGIC_LEN) return false
            for (i in 0..<FlowFileV3.MAGIC_LEN) {
                if (data[i] != FlowFileV3.MAGIC[i]) return false
            }
            return true
        }
    }
}
