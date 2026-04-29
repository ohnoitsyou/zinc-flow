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
import java.util.List
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
 * enabled: true
 * repo: /etc/zincflow        # working tree, defaults to cwd
 * git: /usr/bin/git          # executable, defaults to "git" on PATH
 * remote: origin             # default remote for push
 * branch: main               # default branch for push
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
    class CommandResult(@JvmField val ok: Boolean, @JvmField val exitCode: Int, stdout: String?, stderr: String?) {
        val stdout: String?
        val stderr: String?

        init {
            var stdout = stdout
            var stderr = stderr
            stdout = if (stdout == null) "" else stdout
            stderr = if (stderr == null) "" else stderr
            this.stdout = stdout
            this.stderr = stderr
        }
    }

    @JvmRecord
    data class Status(
        @JvmField val enabled: Boolean, @JvmField val clean: Boolean, @JvmField val ahead: Int, @JvmField val behind: Int,
        val branch: String?, @JvmField val error: String?
    )

    private val repo: Path
    private val gitBinary: String
    private val remote: String
    private val branch: String

    @Volatile
    private var state = ComponentState.DISABLED

    init {
        this.repo = if (repo == null) Path.of(".") else repo
        this.gitBinary = if (gitBinary == null || gitBinary.isEmpty()) "git" else gitBinary
        this.remote = if (remote == null || remote.isEmpty()) "origin" else remote
        this.branch = if (branch == null || branch.isEmpty()) "main" else branch
    }

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

    fun repo(): Path {
        return repo
    }

    fun gitBinary(): String {
        return gitBinary
    }

    fun remote(): String {
        return remote
    }

    fun branch(): String {
        return branch
    }

    fun status(): Status {
        if (!isEnabled()) return Status(false, false, 0, 0, "", "provider disabled")
        try {
            val branchRes = run(List.of<String?>(gitBinary, "rev-parse", "--abbrev-ref", "HEAD"))
            val currentBranch = if (branchRes.ok) branchRes.stdout!!.trim { it <= ' ' } else branch

            val dirty = run(List.of<String?>(gitBinary, "status", "--porcelain"))
            val clean = dirty.ok && dirty.stdout!!.trim { it <= ' ' }.isEmpty()

            var ahead = 0
            var behind = 0
            val revList = run(
                List.of<String?>(
                    gitBinary, "rev-list", "--left-right", "--count",
                    "HEAD..." + remote + "/" + currentBranch
                )
            )
            if (revList.ok) {
                val parts: Array<String?> =
                    revList.stdout!!.trim { it <= ' ' }.split("\\s+".toRegex()).dropLastWhile { it.isEmpty() }
                        .toTypedArray()
                if (parts.size >= 2) {
                    try {
                        ahead = parts[0]!!.toInt()
                        behind = parts[1]!!.toInt()
                    } catch (ignored: NumberFormatException) { /* leave at 0 */
                    }
                }
            }
            return Status(true, clean, ahead, behind, currentBranch, "")
        } catch (ex: Exception) {
            return Status(true, false, 0, 0, "", ex.toString())
        }
    }

    fun commit(relPath: String?, message: String?): CommandResult {
        if (!isEnabled()) return CommandResult(false, -1, "", "provider disabled")
        if (message == null || message.isEmpty()) {
            return CommandResult(false, -1, "", "commit message must not be blank")
        }
        try {
            val add = run(List.of<String?>(gitBinary, "add", if (relPath == null) "." else relPath))
            if (!add.ok) return add
            return run(List.of<String?>(gitBinary, "commit", "-m", message))
        } catch (ex: Exception) {
            return CommandResult(false, -1, "", ex.toString())
        }
    }

    fun push(): CommandResult {
        if (!isEnabled()) return CommandResult(false, -1, "", "provider disabled")
        try {
            return run(List.of<String?>(gitBinary, "push", remote, branch))
        } catch (ex: Exception) {
            return CommandResult(false, -1, "", ex.toString())
        }
    }

    /** Public for tests — lets the test fixture run arbitrary git
     * commands against the configured repo + binary. */
    @Throws(IOException::class, InterruptedException::class)
    fun run(command: MutableList<String?>): CommandResult {
        val pb = ProcessBuilder(command).directory(repo.toFile()).redirectErrorStream(false)
        val proc = pb.start()
        val out = StringBuilder()
        val err = StringBuilder()
        val outReader: Thread = streamInto(proc.getInputStream(), out)
        val errReader: Thread = streamInto(proc.getErrorStream(), err)
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
            return CommandResult(false, -1, out.toString(), "timeout")
        }
        val exit = proc.exitValue()
        val ok = exit == 0
        if (!ok) log.warn("git {} failed (exit {}): {}", command, exit, err.toString().trim { it <= ' ' })
        return CommandResult(ok, exit, out.toString(), err.toString())
    }

    fun statusJson(): MutableMap<String?, Any?> {
        val s = status()
        val out: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        out.put("enabled", s.enabled)
        out.put("clean", s.clean)
        out.put("ahead", s.ahead)
        out.put("behind", s.behind)
        out.put("branch", s.branch)
        out.put("remote", remote)
        out.put("repo", repo.toString())
        if (!s.error!!.isEmpty()) out.put("error", s.error)
        return out
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(VersionControlProvider::class.java)

        const val NAME: String = "version_control"
        const val TYPE: String = "VersionControlProvider"

        private fun streamInto(`in`: InputStream, sink: StringBuilder): Thread {
            // Virtual thread — drains an external process's stdout/stderr.
            // Blocking reads are a textbook fit for Loom and this pattern
            // fires twice per git invocation, so avoiding platform-thread
            // overhead matters under churn.
            return Thread.ofVirtual().name("zinc-flow-git-stream").start(Runnable {
                try {
                    BufferedReader(InputStreamReader(`in`)).use { reader ->
                        var line: String?
                        while ((reader.readLine().also { line = it }) != null) sink.append(line).append('\n')
                    }
                } catch (ignored: IOException) { /* best effort */
                }
            })
        }
    }
}
