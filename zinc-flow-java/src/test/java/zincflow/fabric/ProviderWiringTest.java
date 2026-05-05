package zincflow.fabric;

import org.junit.jupiter.api.Test;
import zincflow.core.ComponentState;
import zincflow.core.FlowFile;
import zincflow.core.MemoryContentStore;
import zincflow.core.Processor;
import zincflow.core.ProcessorContext;
import zincflow.core.ProcessorResult;
import zincflow.core.Source;
import zincflow.providers.ConfigProvider;
import zincflow.providers.ContentProvider;
import zincflow.providers.LoggingProvider;
import zincflow.providers.ProvenanceProvider;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/// Covers the Phase 3e wiring that lives on Pipeline — provider
/// lifecycle, per-processor state, provenance emission, and dynamic
/// add/remove + source registration.
final class ProviderWiringTest {

    private static PipelineGraph singleNoop(Processor p) {
        return new PipelineGraph(Map.of("p", p), Map.of(), List.of("p"));
    }

    @Test
    void provenanceRecordsProcessedEvents() {
        var context = new ProcessorContext();
        var prov = new ProvenanceProvider();
        prov.enable();
        context.addProvider(prov);

        Processor noop = ff -> ProcessorResult.single(ff);
        var pipeline = new Pipeline(singleNoop(noop), Pipeline.DEFAULT_MAX_HOPS, null, context, null);
        pipeline.ingest(FlowFile.create(new byte[0], Map.of()));

        assertEquals(1, prov.size());
        assertEquals(ProvenanceProvider.EventType.PROCESSED, prov.getRecent(1).get(0).type);
    }

    @Test
    void disabledProcessorShortCircuits() {
        var tripped = new AtomicBoolean(false);
        Processor guarded = ff -> { tripped.set(true); return ProcessorResult.single(ff); };

        var pipeline = new Pipeline(singleNoop(guarded));
        pipeline.recordProcessorDef("p", "Guarded", Map.of(), List.of());
        assertTrue(pipeline.disableProcessor("p"));
        pipeline.ingest(FlowFile.create(new byte[0], Map.of()));

        assertFalse(tripped.get(), "disabled processor must not run");
        assertEquals(ComponentState.DISABLED, pipeline.processorState("p"));
    }

    @Test
    void enableProcessorReversesDisable() {
        var runs = new AtomicBoolean(false);
        Processor p = ff -> { runs.set(true); return ProcessorResult.single(ff); };

        var pipeline = new Pipeline(singleNoop(p));
        pipeline.disableProcessor("p");
        pipeline.ingest(FlowFile.create(new byte[0], Map.of())); // dropped
        assertFalse(runs.get());

        pipeline.enableProcessor("p");
        pipeline.ingest(FlowFile.create(new byte[0], Map.of()));
        assertTrue(runs.get());
    }

    @Test
    void disableProviderCascadesToDependents() {
        var context = new ProcessorContext();
        var logging = new LoggingProvider();
        logging.enable();
        context.addProvider(logging);
        context.registerDependent("logging", "logger");

        Processor logger = ff -> ProcessorResult.single(ff);
        var graph = new PipelineGraph(Map.of("logger", logger), Map.of(), List.of("logger"));
        var pipeline = new Pipeline(graph, Pipeline.DEFAULT_MAX_HOPS, null, context, null);

        assertTrue(pipeline.disableProvider("logging"));
        assertEquals(ComponentState.DISABLED, pipeline.processorState("logger"),
                "dependent processor should cascade-disable when its provider goes down");
        assertEquals(ComponentState.DISABLED, logging.state());

        assertTrue(pipeline.enableProvider("logging"));
        assertEquals(ComponentState.ENABLED, logging.state());
    }

    @Test
    void addProcessorUsesRegistry() {
        var registry = new Registry();
        var pipeline = new Pipeline(PipelineGraph.empty(), Pipeline.DEFAULT_MAX_HOPS, null, null, registry);

        assertTrue(pipeline.addProcessor("log", "LogAttribute",
                Map.of("prefix", "[x] "), List.of(), Map.of()));
        assertTrue(pipeline.graph().processors().containsKey("log"));
        assertEquals("LogAttribute", pipeline.processorType("log"));
        assertEquals(ComponentState.ENABLED, pipeline.processorState("log"));

        // Duplicate rejected
        assertFalse(pipeline.addProcessor("log", "LogAttribute", Map.of(), List.of(), Map.of()));
        // Unknown type rejected
        assertFalse(pipeline.addProcessor("mystery", "NoSuchType", Map.of(), List.of(), Map.of()));

        assertTrue(pipeline.removeProcessor("log"));
        assertFalse(pipeline.graph().processors().containsKey("log"));
        assertFalse(pipeline.removeProcessor("log")); // already gone
    }

    @Test
    void sourceLifecycleMirrorsStartStop() {
        var pipeline = new Pipeline(PipelineGraph.empty());
        var fake = new FakeSource("inbox", "http");
        pipeline.addSource(fake);

        assertEquals(1, pipeline.sources().size());
        assertTrue(pipeline.startSource("inbox"));
        assertTrue(fake.isRunning());
        assertTrue(pipeline.stopSource("inbox"));
        assertFalse(fake.isRunning());

        assertFalse(pipeline.startSource("missing"));
        assertFalse(pipeline.stopSource("missing"));
    }

    @Test
    void registryThreadsContextProvidersIntoFactories() {
        var ctx = new ProcessorContext();
        var content = new ContentProvider(new MemoryContentStore());
        content.enable();
        ctx.addProvider(content);

        var registry = new Registry();
        Processor p = registry.create("ExtractText", Map.of("pattern", "."), ctx);
        assertNotNull(p);
    }

    @Test
    void configProviderExposesDotPathAccess() {
        var cfg = new ConfigProvider(Map.of("flow", Map.of("foo", Map.of("bar", 7))));
        assertEquals(7, cfg.getInt("flow.foo.bar", 0));
        assertEquals(42, cfg.getInt("missing.path", 42));
        assertEquals("7", cfg.getString("flow.foo.bar", ""));
    }

    private static final class FakeSource implements Source {
        private final String name;
        private final String type;
        private boolean running;
        FakeSource(String name, String type) { this.name = name; this.type = type; }
        @Override public String name() { return name; }
        @Override public String sourceType() { return type; }
        @Override public boolean isRunning() { return running; }
        @Override public void start(java.util.function.Predicate<zincflow.core.FlowFile> ingest) { running = true; }
        @Override public void stop() { running = false; }
    }
}
