package zincflow.fabric

import java.util.Objects
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/** Running counters for the pipeline. Updated on every processor
 * dispatch, surfaced through `/api/stats`. Every increment also
 * updates the paired [Metrics] registry so `/metrics`
 * stays in sync — Stats always holds a non-null registry (defaults to
 * a fresh [Metrics] when one isn't supplied). */
class Stats @JvmOverloads constructor(metrics: Metrics? = null) {
    private val totalIngested = AtomicLong()
    private val totalProcessed = AtomicLong()
    private val totalDropped = AtomicLong()
    private val totalFailed = AtomicLong()
    private val processorCounts = ConcurrentHashMap<String, AtomicLong>()
    private val processorErrors = ConcurrentHashMap<String, AtomicLong>()

    private val metrics: Metrics = Objects.requireNonNullElseGet(metrics) { Metrics() }

    fun metrics(): Metrics {
        return metrics
    }

    fun recordIngested() {
        totalIngested.incrementAndGet()
        metrics.recordIngested()
    }

    fun recordProcessed(proc: String) {
        totalProcessed.incrementAndGet()
        processorCounts.computeIfAbsent(proc) { AtomicLong() }.incrementAndGet()
        metrics.recordProcessed(proc)
    }

    fun recordDropped() {
        totalDropped.incrementAndGet()
        metrics.recordDropped()
    }

    fun recordFailed(proc: String) {
        totalFailed.incrementAndGet()
        processorErrors.computeIfAbsent(proc) { AtomicLong() }.incrementAndGet()
        metrics.recordFailed(proc)
    }

    fun processorCountsSnapshot(): Map<String, Long> {
        return mapSnapshot(processorCounts)
    }

    fun processorErrorsSnapshot(): Map<String, Long> {
        return mapSnapshot(processorErrors)
    }

    /** Zero the per-processor counters for a single processor. Used by
     * `POST /api/processors/{name}/stats/reset`. Leaves pipeline-wide
     * totals untouched. Returns true if the processor had any counters to
     * reset (always true for processors that have ever been executed). */
    fun resetProcessor(proc: String): Boolean {
        var had = false
        val c = processorCounts[proc]
        if (c != null) {
            c.set(0)
            had = true
        }
        val e = processorErrors[proc]
        if (e != null) {
            e.set(0)
            had = true
        }
        // Seed zero entries so the snapshot shows the processor even pre-run.
        processorCounts.computeIfAbsent(proc) { AtomicLong() }
        processorErrors.computeIfAbsent(proc) { AtomicLong() }
        return had
    }

    /** Snapshot suitable for JSON serialization through Jackson. */
    fun snapshot(): Map<String, Any?> {
        return mapOf(
            "totalIngested" to  totalIngested.get(),
            "totalProcessed" to  totalProcessed.get(),
            "totalDropped" to  totalDropped.get(),
            "totalFailed" to  totalFailed.get(),
            "processorCounts" to  mapSnapshot(processorCounts),
            "processorErrors" to  mapSnapshot(processorErrors)
        )
    }

    companion object {
        private fun mapSnapshot(src: ConcurrentHashMap<String, AtomicLong>): Map<String, Long> {
            return src.entries.associate { (k: String, v: AtomicLong) -> k to v.get() }
        }
    }
}
