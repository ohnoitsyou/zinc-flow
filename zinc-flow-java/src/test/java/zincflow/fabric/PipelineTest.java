package zincflow.fabric;

import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.processors.UpdateAttribute;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

final class PipelineTest {

    /// Captures the last FlowFile it sees so tests can assert on the
    /// state a pipeline delivered to a sink.
    private static final class CapturingSink implements Processor {
        final AtomicInteger calls = new AtomicInteger();
        FlowFile last;

        @Override
        public ProcessorResult process(FlowFile ff) {
            calls.incrementAndGet();
            last = ff;
            return ProcessorResult.dropped();
        }
    }

    @Test
    void ingestFlowsThroughSuccessConnections() {
        var sink = new CapturingSink();
        var graph = new PipelineGraph(
                Map.of(
                        "mark",  new UpdateAttribute("stage", "processed"),
                        "sink",  sink),
                Map.of("mark", Map.of("success", List.of("sink"))),
                List.of("mark"));
        var pipeline = new Pipeline(graph);

        pipeline.ingest(FlowFile.create(new byte[0], Map.of()));

        assertEquals(1, sink.calls.get(), "sink should see one flowfile");
        assertEquals("processed", sink.last.attributes().get("stage"));
        assertEquals(2L, pipeline.stats().snapshot().get("totalProcessed"),
                "stats count both mark + sink dispatches");
    }

    @Test
    void missingEntryPointsWarnButDoNotThrow() {
        var graph = PipelineGraph.empty();
        var pipeline = new Pipeline(graph);
        assertDoesNotThrow(() -> pipeline.ingest(FlowFile.create(new byte[0], Map.of())));
    }

    @Test
    void processorExceptionRoutesToFailureWhenWired() {
        Processor boom = ff -> { throw new RuntimeException("kaboom"); };
        var failureSink = new CapturingSink();
        var graph = new PipelineGraph(
                Map.of("boom", boom, "onFail", failureSink),
                Map.of("boom", Map.of("failure", List.of("onFail"))),
                List.of("boom"));
        var pipeline = new Pipeline(graph);

        pipeline.ingest(FlowFile.create(new byte[0], Map.of()));

        assertEquals(1, failureSink.calls.get(), "failure sink should fire on exception");
    }

    @Test
    void processorExceptionWithNoFailureWiringDropsCleanly() {
        Processor boom = ff -> { throw new RuntimeException("kaboom"); };
        var graph = new PipelineGraph(
                Map.of("boom", boom),
                Map.of(),
                List.of("boom"));
        var pipeline = new Pipeline(graph);
        assertDoesNotThrow(() -> pipeline.ingest(FlowFile.create(new byte[0], Map.of())));
        assertEquals(1L, pipeline.stats().snapshot().get("totalFailed"));
    }

    @Test
    void maxHopsCapsRunawayCycles() {
        // A processor that bumps the stage count and loops back to itself.
        Processor loop = ff -> ProcessorResult.single(ff);
        var graph = new PipelineGraph(
                Map.of("loop", loop),
                Map.of("loop", Map.of("success", List.of("loop"))),
                List.of("loop"));
        var pipeline = new Pipeline(graph, 5);
        assertDoesNotThrow(() -> pipeline.ingest(FlowFile.create(new byte[0], Map.of())),
                "hop cap must protect against cycles");
    }
}
