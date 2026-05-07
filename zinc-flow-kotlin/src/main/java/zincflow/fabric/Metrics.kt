package zincflow.fabric

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Tags
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import zincflow.core.Source
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.ToDoubleFunction

/** Prometheus-backed metrics. Mirrors the [Stats] counters so the
 * /metrics endpoint and /api/stats stay aligned. Per-processor counters
 * use a tag (`processor`) rather than distinct metric names,
 * matching Prometheus idioms.
 * 
 * Metric names track zinc-flow-csharp's `ZincFlow/Fabric/Metrics.cs`
 * so Prometheus scrapers work against either implementation unchanged. */
class Metrics {
    private val registry: PrometheusMeterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

    private val ingested: Counter = Counter.builder("zinc_flow_ingested_total")
        .description("Total FlowFiles accepted at any source")
        .register(registry)
    private val dropped: Counter = Counter.builder("zinc_flow_dropped_total")
        .description("Total FlowFiles dropped by Dropped results or unrouteable failures")
        .register(registry)

    // Per-processor counters are created on demand to avoid unbounded
    // growth when processors come and go via hot reload.
    private val processorCounts = ConcurrentHashMap<String, Counter>()
    private val processorErrors = ConcurrentHashMap<String, Counter>()

    // Gauges registered by Micrometer read through these refs; holding
    // them strongly is required (Micrometer's gauge binding uses weak
    // references on the target object).
    private val activeExecutions = AtomicInteger()
    private val startInstant: Instant = Instant.now()

    // Remember which sources we've already bound a gauge for so hot
    // reload / repeated addSource calls don't double-register.
    private val sourceGaugesBound: MutableSet<String> = ConcurrentHashMap.newKeySet()

    init {
        Gauge.builder("zinc_flow_active_executions", activeExecutions) { a: AtomicInteger -> a.get().toDouble() }
            .description("In-flight pipeline executions")
            .register(registry)
        Gauge.builder("zinc_flow_uptime_seconds", startInstant) { s: Instant? -> (Instant.now().toEpochMilli() - s!!.toEpochMilli()) / 1000.0 }
            .description("Seconds since the worker booted")
            .register(registry)
    }

    /** Expose the registry for Stats' write-through and for alternative
     * scrape endpoints (e.g. micrometer's built-in binders). */
    fun registry(): PrometheusMeterRegistry {
        return registry
    }

    fun scrape(): String {
        return registry.scrape()
    }

    fun recordIngested() {
        ingested.increment()
    }

    fun recordDropped() {
        dropped.increment()
    }

    fun recordProcessed(processor: String) {
        processorCounts.computeIfAbsent(processor) { p: String ->
            Counter.builder("zinc_flow_processor_processed_total")
                .description("FlowFiles dispatched to processor")
                .tags(Tags.of("processor", p))
                .register(registry)
        }.increment()
    }

    fun recordFailed(processor: String) {
        processorErrors.computeIfAbsent(processor) { p: String ->
            Counter.builder("zinc_flow_processor_errors_total")
                .description("Processor invocations that threw or returned Failure")
                .tags(Tags.of("processor", p))
                .register(registry)
        }.increment()
    }

    /** Called at the top of [Pipeline.ingest]. Paired with
     * [.endExecution] in a finally block so the gauge tracks
     * truly in-flight executions even when ingest throws. */
    fun beginExecution() {
        activeExecutions.incrementAndGet()
    }

    fun endExecution() {
        activeExecutions.decrementAndGet()
    }

    /** Current in-flight execution count. Surfaced through
     * `/api/stats` and `/api/flow`'s embedded stats so
     * operators can see live load without waiting for a Prometheus
     * scrape round-trip. */
    fun activeExecutions(): Int {
        return activeExecutions.get()
    }

    /** Bind a running-state gauge for a source. The gauge reads
     * [Source.isRunning] at scrape time, so start/stop
     * transitions don't need explicit metric updates. */
    fun onSourceRegistered(source: Source) {
        val name = source.name()
        if (!sourceGaugesBound.add(name)) return
        Gauge.builder("zinc_flow_source_running", source) { s: Source ->
            if (s.isRunning) 1.0 else 0.0
        }.description("1 when the source is actively polling, 0 when stopped")
            .tags(Tags.of("name", name, "type", source.sourceType()))
            .register(registry)
    }
}
