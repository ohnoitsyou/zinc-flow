package zincflow.core

import java.io.IOException
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.concurrent.atomic.AtomicLong

/** Disk-backed [ContentStore]. Claims live under
 * `baseDir/<2-char-shard>/<claim-id>`, where the shard is the
 * first two characters of the claim id — keeps any one directory from
 * holding millions of files. Writes go to a sibling `.part` file
 * and get atomically moved into place so a crash mid-write can't leave
 * a torn claim for a reader to see.
 * 
 * Mirror of zinc-flow-csharp's FileContentStore, including the
 * `claim-<ticks>-<counter>` id scheme so claims sort by creation
 * time. Thread-safe — [AtomicLong] drives the counter, and the
 * filesystem provides the rest of the atomicity. */
class FileContentStore(baseDir: Path) : ContentStore {
    private val baseDir: Path
    private val counter = AtomicLong()

    init {
        requireNotNull(baseDir) { "FileContentStore: baseDir must not be null" }
        this.baseDir = baseDir
        Files.createDirectories(baseDir)
    }

    fun baseDir(): Path {
        return baseDir
    }

    override fun store(data: ByteArray): String {
        val id = generateClaimId()
        val target = claimPath(id)
        val tmp = target.resolveSibling(target.getFileName().toString() + ".part")
        try {
            Files.createDirectories(target.getParent())
            Files.write(tmp, data)
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE)
        } catch (ex: IOException) {
            throw RuntimeException("FileContentStore: failed to store claim " + id, ex)
        }
        return id
    }

    @Throws(IOException::class)
    override fun store(`in`: InputStream): String {
        val id = generateClaimId()
        val target = claimPath(id)
        val tmp = target.resolveSibling(target.getFileName().toString() + ".part")
        Files.createDirectories(target.getParent())
        Files.newOutputStream(tmp).use { out ->
            `in`.transferTo(out)
        }
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE)
        return id
    }

    override fun retrieve(claimId: String): ByteArray {
        val p = claimPath(claimId)
        if (!Files.exists(p)) return ByteArray(0)
        try {
            return Files.readAllBytes(p)
        } catch (ex: IOException) {
            throw RuntimeException("FileContentStore: retrieve failed", ex)
        }
    }

    @Throws(IOException::class)
    override fun openRead(claimId: String): InputStream {
        return Files.newInputStream(claimPath(claimId))
    }

    override fun size(claimId: String): Long {
        val p = claimPath(claimId)
        if (!Files.exists(p)) return -1
        try {
            return Files.size(p)
        } catch (ex: IOException) {
            return -1
        }
    }

    override fun delete(claimId: String) {
        try {
            Files.deleteIfExists(claimPath(claimId))
        } catch (ignored: IOException) { /* best-effort */
        }
    }

    override fun exists(claimId: String): Boolean {
        return Files.exists(claimPath(claimId))
    }

    private fun claimPath(claimId: String): Path {
        val shard = if (claimId.length >= 2) claimId.substring(0, 2) else "00"
        return baseDir.resolve(shard).resolve(claimId)
    }

    private fun generateClaimId(): String {
        return "claim-" + System.currentTimeMillis() + "-" + counter.incrementAndGet()
    }
}
