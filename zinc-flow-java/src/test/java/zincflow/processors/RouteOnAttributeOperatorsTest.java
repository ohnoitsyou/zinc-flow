package zincflow.processors;

import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;
import zincflow.core.ProcessorResult;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/// Operator-coverage tests for the expanded RouteOnAttribute rule set.
/// Phase 3e adds CONTAINS/STARTSWITH/ENDSWITH/MATCHES/EXISTS and numeric
/// GT/GE/LT/LE on top of the prior EQ/NEQ set.
final class RouteOnAttributeOperatorsTest {

    private static String route(String spec, Map<String, String> attrs) {
        var proc = new RouteOnAttribute(spec);
        var ff = FlowFile.Companion.create(new byte[0], attrs);
        return switch (proc.process(ff)) {
            case ProcessorResult.Routed r -> r.getRoute();
//            case ProcessorResult.Routed(String name, FlowFile ignored) -> name;
            case ProcessorResult r -> {
                fail("expected Routed, got " + r);
                yield "";
            }
        };
    }

    @Test
    void containsOperator() {
        assertEquals("hit", route("hit: msg CONTAINS error", Map.of("msg", "got an error here")));
        assertEquals("unmatched", route("hit: msg CONTAINS error", Map.of("msg", "all clean")));
    }

    @Test
    void startsWithEndsWith() {
        assertEquals("pre", route("pre: path STARTSWITH /api", Map.of("path", "/api/v1")));
        assertEquals("suf", route("suf: path ENDSWITH .json", Map.of("path", "doc.json")));
        assertEquals("unmatched", route("pre: path STARTSWITH /api", Map.of("path", "/users/api")));
    }

    @Test
    void matchesOperator() {
        assertEquals("hex", route("hex: v MATCHES [0-9a-f]{4}", Map.of("v", "dead")));
        assertEquals("unmatched", route("hex: v MATCHES [0-9a-f]{4}", Map.of("v", "DEAD")));
    }

    @Test
    void existsIgnoresValue() {
        assertEquals("present", route("present: tenant EXISTS", Map.of("tenant", "")));
        assertEquals("unmatched", route("present: tenant EXISTS", Map.of()));
    }

    @Test
    void numericComparisonsParseAsDoubles() {
        assertEquals("hi", route("hi: score GT 90", Map.of("score", "95")));
        assertEquals("lo", route("lo: score LT 10", Map.of("score", "3")));
        assertEquals("eq", route("eq: score GE 5", Map.of("score", "5")));
        assertEquals("le", route("le: score LE 5", Map.of("score", "5")));
        assertEquals("unmatched", route("hi: score GT 90", Map.of("score", "50")));
    }

    @Test
    void nonNumericComparisonsFallBackToLexicographic() {
        assertEquals("hi", route("hi: bucket GT m", Map.of("bucket", "n")));
        assertEquals("lo", route("lo: bucket LT m", Map.of("bucket", "a")));
    }

    @Test
    void symbolicOperatorAliases() {
        assertEquals("hi", route("hi: score > 90", Map.of("score", "95")));
        assertEquals("lo", route("lo: score < 10", Map.of("score", "3")));
        assertEquals("eq", route("eq: score == 5", Map.of("score", "5")));
        assertEquals("neq", route("neq: score != 5", Map.of("score", "6")));
    }

    @Test
    void firstMatchingRuleWins() {
        // CONTAINS matches first, so we should get "a" even though the EQ rule
        // on value would also match a later rule.
        String spec = "a: msg CONTAINS warn; b: msg == warning";
        assertEquals("a", route(spec, Map.of("msg", "warning")));
    }

    @Test
    void missingValueForNonExistsOperatorIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new RouteOnAttribute("broken: attr CONTAINS"));
    }

    @Test
    void unknownOperatorIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new RouteOnAttribute("broken: attr WHATEVER x"));
    }
}
