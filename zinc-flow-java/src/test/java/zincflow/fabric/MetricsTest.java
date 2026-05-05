package zincflow.fabric;

import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.core.Source;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

final class MetricsTest {

    @Test
    void scrapeContainsProcessorCounter() {
        Processor noop = ff -> ProcessorResult.dropped();
        var graph = new PipelineGraph(Map.of("noop", noop), Map.of(), List.of("noop"));
        var metrics = new Metrics();
        var pipeline = new Pipeline(graph, Pipeline.DEFAULT_MAX_HOPS, metrics);

        pipeline.ingest(FlowFile.create(new byte[0], Map.of()));
        pipeline.ingest(FlowFile.create(new byte[0], Map.of()));

        String scrape = metrics.scrape();
        assertTrue(scrape.contains("zinc_flow_ingested_total"),
                "expected zinc_flow_ingested_total in metrics, got:\n" + scrape);
        assertTrue(scrape.contains("processor=\"noop\""),
                "expected per-processor tag in scrape output, got:\n" + scrape);
        assertTrue(scrape.contains("zinc_flow_dropped_total 2"),
                "dropped should equal ingest count when sole processor returns Dropped, got:\n" + scrape);
    }

    @Test
    void statsAndMetricsAgree() {
        Processor noop = ff -> ProcessorResult.single(ff);
        var graph = new PipelineGraph(Map.of("noop", noop), Map.of(), List.of("noop"));
        var metrics = new Metrics();
        var pipeline = new Pipeline(graph, Pipeline.DEFAULT_MAX_HOPS, metrics);

        for (int i = 0; i < 5; i++) {
            pipeline.ingest(FlowFile.create(new byte[0], Map.of()));
        }

        var snapshot = pipeline.stats().snapshot();
        assertEquals(5L, snapshot.get("totalIngested"));
        assertEquals(5L, snapshot.get("totalProcessed"));
        assertTrue(metrics.scrape().contains("zinc_flow_ingested_total 5"),
                "metrics registry should reflect 5 ingests");
    }

    @Test
    void defaultPipelineHasMetrics() {
        // Pipeline constructed without an explicit Metrics still has one —
        // a fresh registry is allocated so /metrics always works and the
        // call sites never need to null-check.
        Processor noop = ff -> ProcessorResult.dropped();
        var graph = new PipelineGraph(Map.of("noop", noop), Map.of(), List.of("noop"));
        var pipeline = new Pipeline(graph);
        assertNotNull(pipeline.metrics());
        assertDoesNotThrow(() -> pipeline.ingest(FlowFile.create(new byte[0], Map.of())));
        assertTrue(pipeline.metrics().scrape().contains("zinc_flow_ingested_total 1"));
    }

    @Test
    void uptimeGaugeIsExposed() {
        // Uptime gauge registers at construction and increases monotonically.
        // We assert the series is present and reports a non-negative value;
        // exact value depends on scrape timing so we don't compare numerically.
        var metrics = new Metrics();
        String scrape = metrics.scrape();
        assertTrue(scrape.contains("zinc_flow_uptime_seconds"),
                "expected zinc_flow_uptime_seconds gauge in scrape, got:\n" + scrape);
    }

    @Test
    void activeExecutionsGaugeSettlesAtZero() {
        // Synchronous ingest paths all run to completion on the caller
        // thread, so after N ingests the gauge must read 0.
        Processor noop = ff -> ProcessorResult.single(ff);
        var graph = new PipelineGraph(Map.of("noop", noop), Map.of(), List.of("noop"));
        var metrics = new Metrics();
        var pipeline = new Pipeline(graph, Pipeline.DEFAULT_MAX_HOPS, metrics);

        for (int i = 0; i < 3; i++) {
            pipeline.ingest(FlowFile.create(new byte[0], Map.of()));
        }

        String scrape = metrics.scrape();
        assertTrue(scrape.contains("zinc_flow_active_executions 0"),
                "expected active_executions=0 after sync ingests, got:\n" + scrape);
    }

    @Test
    void activeExecutionsReflectsMidFlightIngest() {
        // Latching processor parks a worker thread so we can assert the
        // gauge from a second thread while the first is mid-ingest.
        var latch = new java.util.concurrent.CountDownLatch(1);
        var released = new java.util.concurrent.CountDownLatch(1);
        Processor blocker = ff -> {
            latch.countDown();
            try { released.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return ProcessorResult.single(ff);
        };
        var graph = new PipelineGraph(Map.of("blocker", blocker), Map.of(), List.of("blocker"));
        var metrics = new Metrics();
        var pipeline = new Pipeline(graph, Pipeline.DEFAULT_MAX_HOPS, metrics);

        var worker = Thread.ofVirtual().start(() ->
                pipeline.ingest(FlowFile.create(new byte[0], Map.of())));

        try { latch.await(); } catch (InterruptedException e) { fail("interrupted waiting for ingest start"); }
        String midScrape = metrics.scrape();
        assertTrue(midScrape.contains("zinc_flow_active_executions 1"),
                "expected active_executions=1 while processor is blocked, got:\n" + midScrape);
        released.countDown();
        try { worker.join(); } catch (InterruptedException e) { fail("interrupted waiting for worker"); }

        assertTrue(metrics.scrape().contains("zinc_flow_active_executions 0"),
                "expected gauge back to 0 after ingest completes");
    }

    @Test
    void sourceRunningGaugeTracksSource() {
        var metrics = new Metrics();
        var stub = new StubSource("stub-a", "GenerateFlowFile");

        Processor noop = ff -> ProcessorResult.dropped();
        var graph = new PipelineGraph(Map.of("noop", noop), Map.of(), List.of("noop"));
        var pipeline = new Pipeline(graph, Pipeline.DEFAULT_MAX_HOPS, metrics);
        pipeline.addSource(stub);

        String stoppedScrape = metrics.scrape();
        assertTrue(stoppedScrape.contains("zinc_flow_source_running"),
                "expected source_running gauge in scrape, got:\n" + stoppedScrape);
        assertTrue(stoppedScrape.contains("name=\"stub-a\"") && stoppedScrape.contains("type=\"GenerateFlowFile\""),
                "expected name+type tags on source_running, got:\n" + stoppedScrape);

        stub.running = true;
        String runningScrape = metrics.scrape();
        // Gauge reads isRunning() at scrape time — must reflect the
        // true state, not the value at registration.
        assertTrue(runningScrape.matches("(?s).*zinc_flow_source_running\\{[^}]*\\} 1(\\.0)?.*"),
                "expected source_running=1 while source is running, got:\n" + runningScrape);
    }

    private static final class StubSource implements Source {
        private final String name;
        private final String type;
        volatile boolean running;

        StubSource(String name, String type) { this.name = name; this.type = type; }

        @Override public String name() { return name; }
        @Override public String sourceType() { return type; }
        @Override public boolean isRunning() { return running; }
        @Override public void start(Predicate<FlowFile> ingest) { running = true; }
        @Override public void stop() { running = false; }
    }
}
