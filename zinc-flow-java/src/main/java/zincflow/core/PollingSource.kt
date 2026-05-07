package zincflow.core

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.function.Predicate
import kotlin.concurrent.Volatile

/** Abstract base for sources that wake up on a fixed interval, scan
 * an external system, and hand resulting FlowFiles to the pipeline.
 * Subclasses implement [.poll]; the base class owns the
 * lifecycle, the virtual-thread loop, and the ingest-accept/reject
 * dispatch.
 * 
 * <h2>Threading</h2>
 * The poll loop runs on a single virtual thread. Blocking I/O inside
 * [.poll] (disk scan, HTTP GET, database query) is fine — it
 * doesn't tie up a platform thread. Stop is cooperative: [.stop]
 * interrupts the thread and [.poll] should return promptly
 * when `Thread.interrupted()` is observed.
 * 
 * <h2>Backpressure</h2>
 * The `ingest` callback passed to [.start] returns
 * `true` if the pipeline accepted the FlowFile. Subclasses react
 * via [.onIngested] (e.g. move the file to
 * `.processed/`) or [.onRejected].
 * 
 * Mirrors zinc-flow-csharp's PollingSource. */
abstract class PollingSource protected constructor(private val name: String, pollIntervalMillis: Long) : Source {
    // Guard against zero/negative — a tight loop would pin a CPU
    // and surprise any operator who typo'd a config value.
    private val pollIntervalMillis: Long = pollIntervalMillis.takeIf { it > 0 } ?: 1000

    @Volatile
    protected var running = false

    @Volatile
    private var loop: Thread? = null

    init {
        require(name.isNotEmpty()) { "source name must not be blank" }
    }

    override fun name(): String = name

    fun pollIntervalMillis(): Long = pollIntervalMillis

    /** Scan the external system and return zero or more FlowFiles to
     * hand to the pipeline. Implementations should return promptly on
     * interruption so [.stop] isn't blocked by a long scan. */
    protected abstract fun poll(): MutableList<FlowFile>

    /** Called after the pipeline accepted a FlowFile. Default is a no-op;
     * `GetFile` overrides this to move the source file to
     * `.processed/`. */
    protected open fun onIngested(ff: FlowFile) { /* default: nothing */
    }

    /** Called when the pipeline refused a FlowFile (ingest returned
     * `false`). Default logs at debug — subclasses can retry,
     * quarantine, or drop. */
    protected open fun onRejected(ff: FlowFile) {
        log.debug("source $name: pipeline rejected ${ff.stringId()}")
    }

    @Synchronized
    override fun start(ingest: (FlowFile) -> Boolean) {
        if (running) return
        running = true
        loop = Thread.ofVirtual().name("zinc-flow-source-$name").start { runLoop(ingest) }
        log.info("source $name started (${sourceType()} type, poll=${pollIntervalMillis}ms)")
    }

    @Synchronized
    override fun stop() {
        if (!running) return
        running = false
        loop?.interrupt() ?: return
        loop = null
        log.info("source $name stopped")
    }

    private fun runLoop(ingest: (FlowFile) -> Boolean) {
        while (running && !Thread.currentThread().isInterrupted) {
            try {
                val batch = poll()
                for (ff in batch) {
                    if (!running) return
                    val accepted: Boolean = try {
                        ingest(ff)
                    } catch (ex: RuntimeException) {
                        log.warn("source $name: ingest threw for ${ff.stringId()} — $ex")
                        false
                    }
                    if (accepted) onIngested(ff)
                    else onRejected(ff)
                }
            } catch (ex: RuntimeException) {
                log.warn("source $name: poll failed — $ex")
            }
            try {
                TimeUnit.MILLISECONDS.sleep(pollIntervalMillis)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            }
        }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(PollingSource::class.java)
    }
}
