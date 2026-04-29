package zincflow.fabric

import java.util.Map
import java.util.Objects
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.function.BiConsumer
import java.util.function.Function
import java.util.function.Supplier

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
    private val processorCounts = ConcurrentHashMap<String?, AtomicLong?>()
    private val processorErrors = ConcurrentHashMap<String?, AtomicLong?>()

    private val metrics: Metrics

    init {
        this.metrics = Objects.requireNonNullElseGet<Metrics>(metrics, Supplier { Metrics() })
    }

    fun metrics(): Metrics {
        return metrics
    }

    fun recordIngested() {
        totalIngested.incrementAndGet()
        metrics.recordIngested()
    }

    fun recordProcessed(proc: String?) {
        totalProcessed.incrementAndGet()
        processorCounts.computeIfAbsent(proc, java.util.function.Function { `_`: kotlin.String? -> AtomicLong() })!!
            .incrementAndGet()
        metrics.recordProcessed(proc)
    }

    fun recordDropped() {
        totalDropped.incrementAndGet()
        metrics.recordDropped()
    }

    fun recordFailed(proc: String?) {
        totalFailed.incrementAndGet()
        processorErrors.computeIfAbsent(proc, java.util.function.Function { `_`: kotlin.String? -> AtomicLong() })!!
            .incrementAndGet()
        metrics.recordFailed(proc)
    }

    fun processorCountsSnapshot(): MutableMap<String?, Long?> {
        return mapSnapshot(processorCounts)
    }

    fun processorErrorsSnapshot(): MutableMap<String?, Long?> {
        return mapSnapshot(processorErrors)
    }

    /** Zero the per-processor counters for a single processor. Used by
     * `POST /api/processors/{name}/stats/reset`. Leaves pipeline-wide
     * totals untouched. Returns true if the processor had any counters to
     * reset (always true for processors that have ever been executed). */
    fun resetProcessor(proc: String?): Boolean {
        var had = false
        val c = processorCounts.get(proc)
        if (c != null) {
            c.set(0)
            had = true
        }
        val e = processorErrors.get(proc)
        if (e != null) {
            e.set(0)
            had = true
        }
        // Seed zero entries so the snapshot shows the processor even pre-run.
        processorCounts.computeIfAbsent(proc, Function { `_`: String? -> AtomicLong() })
        processorErrors.computeIfAbsent(proc, Function { `_`: String? -> AtomicLong() })
        return had
    }

    /** Snapshot suitable for JSON serialization through Jackson. */
    fun snapshot(): MutableMap<String?, Any?> {
        return Map.of<String?, Any?>(
            "totalIngested", totalIngested.get(),
            "totalProcessed", totalProcessed.get(),
            "totalDropped", totalDropped.get(),
            "totalFailed", totalFailed.get(),
            "processorCounts", mapSnapshot(processorCounts),
            "processorErrors", mapSnapshot(processorErrors)
        )
    }

    companion object {
        private fun mapSnapshot(src: ConcurrentHashMap<String?, AtomicLong?>): MutableMap<String?, Long?> {
            val out: ConcurrentHashMap<String?, Long?> = ConcurrentHashMap<String?, Long?>(src.size())
            src.forEach(BiConsumer { k: String?, v: AtomicLong? -> out.put(k, v!!.get()) })
            return Map.copyOf<String?, Long?>(out)
        }
    }
}
