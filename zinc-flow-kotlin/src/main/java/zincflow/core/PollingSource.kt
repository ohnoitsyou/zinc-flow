package zincflow.core

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.function.Predicate
import kotlin.concurrent.Volatile
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration.Companion.milliseconds

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
abstract class PollingSource protected constructor(private val name: String, pollIntervalMillis: Long) : Source, AutoCloseable, CoroutineScope {
    // Guard against zero/negative — a tight loop would pin a CPU
    // and surprise any operator who typo'd a config value.
    private val pollIntervalMillis: Long = pollIntervalMillis.takeIf { it > 0 } ?: 1000

    override val isRunning: Boolean
        get() = currentJob?.isActive ?: false

    private var currentJob: Job? = null

//    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    override val coroutineContext: CoroutineContext = SupervisorJob() + Dispatchers.IO

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
        if (isRunning) return
        this.currentJob = launch(CoroutineName("source-$name")) {
            runLoop(ingest)
            log.info("source $name started (${sourceType()} type, poll=${pollIntervalMillis}ms)")
        }
    }

    @Synchronized
    override fun stop() {
        if (isRunning) cancel()
        log.info("source $name stopped")
    }

    private suspend fun runLoop(ingest: (FlowFile) -> Boolean) {
        while(currentCoroutineContext().isActive) {
            try {
                val batch = poll()
                for (ff in batch) {
                    currentCoroutineContext().ensureActive()
                    val accepted = try {
                        ingest(ff)
                    } catch (ex: RuntimeException) {
                        log.warn("source $name: ingest threw for ${ff.stringId()} — $ex")
                        false
                    }
                    if (accepted) onIngested(ff)
                    else onRejected(ff)
                }
                delay(pollIntervalMillis.milliseconds)
            } catch (ex: RuntimeException) {
                log.warn("source $name: poll failed — $ex")
            } catch (_: CancellationException) {
                cancel()
                return
            }
        }
    }

    override fun close() {
        cancel()
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(PollingSource::class.java)
    }
}
