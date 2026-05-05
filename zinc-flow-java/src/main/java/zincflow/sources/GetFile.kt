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

    private fun emitFor(file: Path): List<FlowFile> {
        val bytes: ByteArray = try {
            Files.readAllBytes(file)
        } catch (ex: IOException) {
            // File might have been removed or partially written between
            // the listing and the read — skip this round, the next poll
            // picks it up.
            log.debug("GetFile ${name()}: could not read $file ($ex) — skipping")
            return mutableListOf()
        }

        if (unpackV3 && looksLikeV3(bytes)) {
            val frames = FlowFileV3.unpackAll(bytes)
            if (frames.isNotEmpty()) {
                return frames.mapIndexed { idx, frame -> addAttrs(frame, file, bytes.size.toLong(), true, idx, frames.size) }
            }
        }

        return listOf(
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
        base: FlowFile,
        file: Path,
        rawSize: Long,
        v3: Boolean,
        frameIndex: Int,
        frameCount: Int
    ): FlowFile {
        val attrs = buildMap {
            put(FlowFileAttributes.FILENAME, file.fileName.toString())
            put(FlowFileAttributes.PATH, file.toAbsolutePath().toString())
            put(FlowFileAttributes.SOURCE, name())
            if (v3) {
                put(FlowFileAttributes.V3_FRAME_INDEX, frameIndex.toString())
                put(FlowFileAttributes.V3_FRAME_COUNT, frameCount.toString())
            } else {
                put(FlowFileAttributes.SIZE, rawSize.toString())
            }
        }
        return FlowFile(base.id, attrs, base.content, base.timestampMillis, base.hopCount)
    }

    override fun onIngested(ff: FlowFile) {
        val file = pendingMoves.remove(ff.id) ?: return
        val remaining = outstandingPerFile.computeIfPresent(file) { _: Path?, v: Int? -> v!! - 1 }
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
        } catch (_: IOException) {
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
            } catch (ex: IOException) {
                log.warn("GetFile ${name()}: failed to move $file to $processedDir — $ex")
            }
        }
    }

    private fun ensureDirs() {
        try {
            Files.createDirectories(inputDir)
        } catch (_: IOException) { /* tolerate — poll will surface issues */
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

        override fun configKeys(): List<String> {
            return listOf("inputDir", "pattern", "pollIntervalMs", "unpackV3")
        }

        override fun create(name: String, config: Map<String, Any>): Source? {
            val inputDir: String = config["inputDir"]?.toString() ?: ""
            if (inputDir.isEmpty()) return null // disabled when inputDir is absent

            return GetFile(
                name,
                Path.of(inputDir),
                config.getOrDefault("pattern", "*").toString(),
                config["pollIntervalMs"] as? Long ?: 1000,
                config["unpackV3"] as? Boolean ?: true,
                false
            )
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
