package zincflow.core

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

/** Tracks every [ClaimContent] currently in-flight through the
 * pipeline and periodically asks a [ContentStore] to delete
 * claims that are no longer referenced. Without this, claims would
 * accumulate in [FileContentStore] forever — a
 * ContentStore has no built-in TTL, and the FlowFile that owned a
 * claim may be long gone by the time we notice.
 * 
 * Thread-safe. The active set is a [HashSet] under a monitor —
 * claims come and go often enough that a copy-on-write set would
 * churn more than the contention costs. Sweeps are light I/O (one
 * `delete` per orphaned claim), so serializing them keeps the
 * codepath simple.
 * 
 * Mirror of zinc-flow-csharp's ContentStoreCleanup. */
class ContentStoreCleanup(private val store: ContentStore) {
    private val active = mutableSetOf<String>()
    private val lock = Any()
    private var scheduler: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("zinc-flow-content-cleanup").factory())

    private var running: ScheduledFuture<*>? = null

    /**
     * Register a claim that's now live. Safe to call from any thread
     * — typically from [ContentHelpers.maybeOffload] or wherever
     * else a new ClaimContent is created.
     */
    fun track(claimId: String) {
        if (claimId.isEmpty()) return
        synchronized(lock) { active.add(claimId) }
    }

    /**
     * Mark a claim as releasable. The next sweep will delete it from
     * the store if it isn't re-tracked before then.
     */
    fun release(claimId: String) {
        synchronized(lock) { active.remove(claimId) }
    }

    fun activeCount(): Int {
        synchronized(lock) { return active.size }
    }

    /**
     * Delete any claim known to the store that isn't currently in the
     * active set. Returns the number deleted — callers can log or
     * metric-ize this without having to subscribe to logging.
     */
    fun sweep(knownClaims: MutableList<String>): Int {
        if (knownClaims.isEmpty()) return 0
        val snapshot = synchronized(lock) { active.toSet() }
        val matchingClaims = snapshot intersect knownClaims.toSet()
        return knownClaims.fold(0) { acc, claim ->
            if(!snapshot.contains(claim)) {
                try {
                    store.delete(claim)
                    acc.inc()
                } catch (ex: RuntimeException) {
                    log.warn("content-cleanup: failed to delete $claim — $ex")
                    acc
                }
            } else {
                acc
            }
        }
    }

    // TODO: Convert to use coroutines
    /**
     * Start a periodic sweep. Caller provides a [ClaimEnumerator]
     * that returns every claim the store currently holds — disk-backed
     * stores walk the directory, memory stores enumerate the map.
     * Running more than once is idempotent: the previous schedule is
     * cancelled first.
     */
    fun startPeriodicSweep(period: Long, unit: TimeUnit, enumerator: ClaimEnumerator) {
        stopPeriodicSweep()
        // Scheduler thread stays platform (STPE timing relies on
        // park/unpark). The sweep body runs on a virtual thread so a
        // slow enumerator or a disk hiccup on delete can't stall the
        // next tick.
        scheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("zinc-flow-content-cleanup").factory())
        running = scheduler.scheduleAtFixedRate(
            { Thread.startVirtualThread { runSweep(enumerator) } },
            period, period, unit
        )
    }

    private fun runSweep(enumerator: ClaimEnumerator) {
        try {
            val deleted = sweep(enumerator.enumerate())
            if (deleted > 0) {
                log.info("content-cleanup: swept $deleted orphaned claim(s)")
            } else {
                log.info("content-cleanup: no orphaned files to sweep")
            }
        } catch (ex: RuntimeException) {
            log.warn("content-cleanup: sweep failed — $ex")
        }
    }

    fun stopPeriodicSweep() {
        running?.cancel(false)
        scheduler.shutdown()
    }

    fun interface ClaimEnumerator {
        fun enumerate(): MutableList<String>
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(ContentStoreCleanup::class.java)
    }
}
