package zincflow.providers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import zincflow.core.ComponentState
import zincflow.core.Provider
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.concurrent.Volatile

/** Shells out to the system `git` binary so the worker can
 * commit + push its own `config.yaml` after a UI-driven edit.
 * JGit intentionally not embedded — ops teams keep control of auth /
 * hooks / refresh-token rotation through their existing git config.
 * 
 * Enable path (config.yaml):
 * <pre>
 * vc:
 *   enabled: true
 *   repo: /etc/zincflow        # working tree, defaults to cwd
 *   git: /usr/bin/git          # executable, defaults to "git" on PATH
 *   remote: origin             # default remote for push
 *   branch: main               # default branch for push
</pre> * 
 * 
 * Operations:
 * - [.status]   — clean flag + ahead/behind + current branch
 * - [.commit] — stage + commit one path
 * - [.push]     — push to configured remote / branch
 * 
 * All commands run with a short timeout and stderr captured. Results
 * are surfaced as records so the admin API can echo them as JSON. */
class VersionControlProvider(repo: Path?, gitBinary: String?, remote: String?, branch: String?) : Provider {
    @JvmRecord
    data class CommandResult @JvmOverloads constructor(@JvmField val ok: Boolean, @JvmField val exitCode: Int, @JvmField val stdout: String = "", @JvmField val stderr: String = "") { }

    @JvmRecord
    data class Status(@JvmField val enabled: Boolean, @JvmField val clean: Boolean, @JvmField val ahead: Int, @JvmField val behind: Int, @JvmField val branch: String, @JvmField val error: String?)

    val repo: Path = repo ?: Path.of(".")
    val gitBinary: String = if (gitBinary.isNullOrEmpty()) "git" else gitBinary
    val remote: String = if (remote.isNullOrEmpty()) "origin" else remote
    val branch: String = if (branch.isNullOrEmpty()) "main" else branch

    @Volatile
    private var state = ComponentState.DISABLED

    override fun name(): String {
        return NAME
    }

    override fun providerType(): String {
        return TYPE
    }

    override fun state(): ComponentState {
        return state
    }

    override fun enable() {
        state = ComponentState.ENABLED
    }

    override fun disable(drainTimeoutSeconds: Int) {
        state = ComponentState.DISABLED
    }

    override fun shutdown() {
        state = ComponentState.DISABLED
    }

    fun status(): Status {
        if (!isEnabled) return Status(!ENABLED, !CLEAN, 0, 0, "", "provider disabled")
        try {
            val branchRes = run(listOf(gitBinary, "rev-parse", "--abbrev-ref", "HEAD"))
            val currentBranch = if (branchRes.ok) branchRes.stdout.trim() else branch

            val dirty = run(listOf(gitBinary, "status", "--porcelain"))
            val clean = dirty.ok && dirty.stdout.trim().isEmpty()

            var ahead = 0
            var behind = 0
            val revList = run(listOf(gitBinary, "rev-list", "--left-right", "--count", "HEAD...$remote/$currentBranch"))
            if (revList.ok) {
                val parts = revList.stdout.trim().split("\\s+".toRegex()).dropLastWhile { it.isEmpty() }
                if (parts.size >= 2) {
                    try {
                        ahead = parts[0].toInt()
                        behind = parts[1].toInt()
                    } catch (_: NumberFormatException) { /* leave at 0 */
                    }
                }
            }
            return Status(ENABLED, clean, ahead, behind, currentBranch, "")
        } catch (ex: Exception) {
            return Status(ENABLED, !CLEAN, 0, 0, "", ex.toString())
        }
    }

    fun commit(relPath: String?, message: String?): CommandResult {
        if (!isEnabled) return CommandResult(!OK, -1, "", "provider disabled")
        if (message.isNullOrEmpty()) {
            return CommandResult(!OK, -1, "", "commit message must not be blank")
        }
        try {
            val add = run(listOf(gitBinary, "add", relPath ?: "."))
            if (!add.ok) return add
            return run(listOf(gitBinary, "commit", "-m", message))
        } catch (ex: Exception) {
            return CommandResult(!OK, -1, "", ex.toString())
        }
    }

    fun push(): CommandResult {
        if (!isEnabled) return CommandResult(!OK, -1, "", "provider disabled")
        return try {
            run(listOf(gitBinary, "push", remote, branch))
        } catch (ex: Exception) {
            CommandResult(!OK, -1, "", ex.toString())
        }
    }

    /** Public for tests — lets the test fixture run arbitrary git
     * commands against the configured repo + binary. */
    @Throws(IOException::class, InterruptedException::class)
    fun run(command: List<String>): CommandResult {
        val pb = ProcessBuilder(command).directory(repo.toFile()).redirectErrorStream(false)
        val proc = pb.start()
        val out = StringBuilder()
        val err = StringBuilder()
        val outReader: Thread = streamInto(proc.inputStream, out)
        val errReader: Thread = streamInto(proc.errorStream, err)
        val done = proc.waitFor(15, TimeUnit.SECONDS)
        if (!done) proc.destroyForcibly()
        // Once the process exits (or is destroyed) the OS closes its
        // stdout/stderr, so the drain threads see EOF and terminate
        // promptly. Joining without a timeout guarantees both buffers
        // are fully populated before we read them — a short timeout
        // would risk returning truncated output under load.
        outReader.join()
        errReader.join()
        if (!done) {
            return CommandResult(!OK, -1, out.toString(), "timeout")
        }
        val exit = proc.exitValue()
        val ok = exit == 0
        if (!ok) log.warn("git {} failed (exit {}): {}", command, exit, err.toString().trim { it <= ' ' })
        return CommandResult(ok, exit, out.toString(), err.toString())
    }

    fun statusJson(): Map<String, Any> {
        val s = status()
        return buildMap {
            put("enabled", s.enabled)
            put("clean", s.clean)
            put("ahead", s.ahead)
            put("behind", s.behind)
            put("branch", s.branch)
            put("remote", remote)
            put("repo", repo.toString())
            if (s.error?.isNotEmpty() == true) put("error", s.error)
        }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(VersionControlProvider::class.java)

        const val NAME: String = "version_control"
        const val TYPE: String = "VersionControlProvider"

        private const val OK = true
        private const val CLEAN = true
        private const val ENABLED = true

        private fun streamInto(`in`: InputStream, sink: StringBuilder): Thread {
            // Virtual thread — drains an external process's stdout/stderr.
            // Blocking reads are a textbook fit for Loom and this pattern
            // fires twice per git invocation, so avoiding platform-thread
            // overhead matters under churn.
            return Thread.ofVirtual().name("zinc-flow-git-stream").start {
                try {
                    BufferedReader(InputStreamReader(`in`)).use { reader ->
                        var line: String?
                        while ((reader.readLine().also { line = it }) != null) sink.append(line).append('\n')
                    }
                } catch (_: IOException) { /* best effort */ }
            }
        }
    }
}
