package zincflow.fabric;

import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/// Unit tests for the Pipeline-level graph mutation API:
/// addConnection, removeConnection, setConnections, setEntryPoints.
/// HTTP-level coverage lives in GraphMutationHttpTest.
final class GraphMutationTest {

    private static Processor passThrough() {
        return ff -> ProcessorResult.single(ff);
    }

    private static Pipeline threeNodePipeline() {
        var graph = new PipelineGraph(
                Map.of("a", passThrough(), "b", passThrough(), "c", passThrough()),
                Map.of("a", Map.of("success", List.of("b"))),
                List.of("a"));
        return new Pipeline(graph);
    }

    @Test
    void addConnectionWiresNewEdge() {
        var p = threeNodePipeline();
        var r = p.addConnection("a", "alt", "c");
        assertTrue(r.ok, r.reason);
        assertEquals(List.of("c"), p.graph().next("a", "alt"));
        assertEquals(List.of("b"), p.graph().next("a", "success"));
    }

    @Test
    void addConnectionRejectsUnknownProcessor() {
        var p = threeNodePipeline();
        assertFalse(p.addConnection("ghost", "success", "b").ok);
        assertFalse(p.addConnection("a", "success", "ghost").ok);
    }

    @Test
    void addConnectionRejectsDuplicateEdge() {
        var p = threeNodePipeline();
        var r = p.addConnection("a", "success", "b");
        assertFalse(r.ok);
        assertTrue(r.reason.contains("already exists"));
    }

    @Test
    void removeConnectionDropsEdgeAndEmptyRelationship() {
        var p = threeNodePipeline();
        assertTrue(p.removeConnection("a", "success", "b").ok);
        assertEquals(List.of(), p.graph().next("a", "success"));
        assertFalse(p.graph().connections().containsKey("a"),
                "removing the last edge under 'a' should drop the key entirely");
    }

    @Test
    void removeConnectionRejectsMissingEdge() {
        var p = threeNodePipeline();
        var r = p.removeConnection("a", "success", "c"); // c isn't on success
        assertFalse(r.ok);
        assertTrue(r.reason.contains("not found"));
    }

    @Test
    void setConnectionsReplacesOutboundSet() {
        var p = threeNodePipeline();
        var r = p.setConnections("a", Map.of(
                "high", List.of("b"),
                "low",  List.of("c")));
        assertTrue(r.ok, r.reason);
        assertEquals(List.of("b"), p.graph().next("a", "high"));
        assertEquals(List.of("c"), p.graph().next("a", "low"));
        assertEquals(List.of(), p.graph().next("a", "success"),
                "setConnections must replace, not merge — old 'success' gone");
    }

    @Test
    void setConnectionsEmptyMapClearsOutbound() {
        var p = threeNodePipeline();
        assertTrue(p.setConnections("a", Map.of()).ok);
        assertFalse(p.graph().connections().containsKey("a"));
    }

    @Test
    void setConnectionsValidatesTargetsBeforeCommit() {
        var p = threeNodePipeline();
        var before = p.graph();
        var r = p.setConnections("a", Map.of("bad", List.of("ghost")));
        assertFalse(r.ok);
        assertSame(before, p.graph(), "invalid edit must not swap the graph");
    }

    @Test
    void setEntryPointsReplacesSet() {
        var p = threeNodePipeline();
        assertTrue(p.setEntryPoints(List.of("b", "c")).ok);
        assertEquals(List.of("b", "c"), p.graph().entryPoints());
    }

    @Test
    void setEntryPointsRejectsUnknownName() {
        var p = threeNodePipeline();
        var before = p.graph();
        assertFalse(p.setEntryPoints(List.of("ghost")).ok);
        assertSame(before, p.graph());
    }

    @Test
    void mutationsTakeEffectForSubsequentIngests() {
        // End-to-end: wire a new branch dynamically, verify a FlowFile
        // ingested after the edit flows through it.
        var hits = new java.util.concurrent.atomic.AtomicInteger();
        Processor ingress = ff -> ProcessorResult.single(ff);
        Processor side = ff -> { hits.incrementAndGet(); return ProcessorResult.dropped(); };

        var graph = new PipelineGraph(
                Map.of("ingress", ingress, "side", side),
                Map.of(),
                List.of("ingress"));
        var p = new Pipeline(graph);

        p.ingest(FlowFile.create(new byte[0], Map.of()));
        assertEquals(0, hits.get(), "no connection yet — side processor should not run");

        assertTrue(p.addConnection("ingress", "success", "side").ok);
        p.ingest(FlowFile.create(new byte[0], Map.of()));
        assertEquals(1, hits.get(), "after addConnection, flowfile reaches side");
    }
}
