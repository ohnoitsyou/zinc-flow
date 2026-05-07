package zincflow.fabric;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class FlowValidatorTest {

    @Test
    void straightLineGraphValidatesWithNoWarnings() {
        var r = FlowValidator.validate(
                List.of("a", "b", "c"),
                Map.of(
                        "a", Map.of("success", List.of("b")),
                        "b", Map.of("success", List.of("c"))));
        assertTrue(r.ok());
        assertTrue(r.getWarnings().isEmpty());
        assertEquals(List.of("a"), r.getEntryPoints());
    }

    @Test
    void unknownTargetProducesError() {
        var r = FlowValidator.validate(
                List.of("a"),
                Map.of("a", Map.of("success", List.of("ghost"))));
        assertFalse(r.ok());
        assertTrue(r.getErrors().stream().anyMatch(e -> e.contains("ghost")));
    }

    @Test
    void cycleDetectedAsWarning() {
        var r = FlowValidator.validate(
                List.of("a", "b", "c"),
                Map.of(
                        "a", Map.of("success", List.of("b")),
                        "b", Map.of("success", List.of("c")),
                        "c", Map.of("success", List.of("a"))));
        assertTrue(r.ok(), "cycles are warnings, not errors");
        assertTrue(r.getWarnings().stream().anyMatch(w -> w.startsWith("cycle detected")),
                "expected cycle warning, got " + r.getWarnings());
    }

    @Test
    void unreachableProcessorWarns() {
        var r = FlowValidator.validate(
                List.of("entry", "island"),
                Map.of("entry", Map.of()));
        assertTrue(r.ok());
        // entry has no inbound = entry point; island has no inbound = also
        // an entry point. No unreachable warning since both are entry
        // points. This verifies the definition is "no inbound edges".
        assertEquals(2, r.getEntryPoints().size());
        assertTrue(r.getEntryPoints().contains("entry"));
        assertTrue(r.getEntryPoints().contains("island"));
    }

    @Test
    void unreachableProcessorWithInboundEdgeWarns() {
        // A processor with an inbound edge but no path from any entry point
        // — possible when a sub-cycle is disconnected from the entry set.
        var r = FlowValidator.validate(
                List.of("a", "b", "c"),
                Map.of(
                        "a", Map.of(),
                        "b", Map.of("success", List.of("c")),
                        "c", Map.of("success", List.of("b"))));
        assertTrue(r.getWarnings().stream().anyMatch(w -> w.contains("not reachable")),
                "expected unreachable warning, got " + r.getWarnings());
    }

    @Test
    void multiRelationshipGraph() {
        var r = FlowValidator.validate(
                List.of("router", "high", "low"),
                Map.of(
                        "router", Map.of(
                                "high", List.of("high"),
                                "low",  List.of("low"))));
        assertTrue(r.ok());
        assertTrue(r.getWarnings().isEmpty());
        assertEquals(List.of("router"), r.getEntryPoints());
    }

    @Test
    void allErrorsAccumulatedNotJustFirst() {
        var r = FlowValidator.validate(
                List.of("a"),
                Map.of("a", Map.of(
                        "success", List.of("ghost1", "ghost2"),
                        "failure", List.of("ghost3"))));
        assertEquals(3, r.getErrors().size(),
                "every unknown target should surface, not just the first");
    }

    @Test
    void emptyGraphValidates() {
        var r = FlowValidator.validate(List.of(), Map.of());
        assertTrue(r.ok());
        assertTrue(r.getWarnings().isEmpty());
        assertTrue(r.getEntryPoints().isEmpty());
    }
}
