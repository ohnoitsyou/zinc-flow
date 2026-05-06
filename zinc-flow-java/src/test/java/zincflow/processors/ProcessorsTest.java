package zincflow.processors;

import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;
import zincflow.core.ProcessorResult;
import zincflow.core.RawContent;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class ProcessorsTest {

    private static FlowFile singleOut(ProcessorResult r) {
        return switch (r) {
            case ProcessorResult.Single ff -> ff.flowFile;
            default -> {
                fail("expected Single, got " + r);
                yield null;
            }
        };
    }

    private static String routeOf(ProcessorResult r) {
        return switch (r) {
            case ProcessorResult.Routed route -> route.getRoute();
//            case ProcessorResult.Routed(String name, FlowFile ignored) -> name;
            default -> {
                fail("expected Routed, got " + r);
                yield "";
            }
        };
    }

    private static List<FlowFile> multiOut(ProcessorResult r) {
        return switch (r) {
            case ProcessorResult.Multiple ffs -> ffs.getFlowFiles();
            default -> {
                fail("expected Multiple, got " + r);
                yield List.of();
            }
        };
    }

    // --- UpdateAttribute ---

    @Test
    void updateAttributeSetsKeyAndWrapsInSingle() {
        var ff = FlowFile.Companion.create(new byte[0], Map.of());
        var out = singleOut(new UpdateAttribute("priority", "high").process(ff));
        assert out != null;
        assertEquals("high", out.getAttributes().get("priority"));
    }

    @Test
    void updateAttributeBlankKeyRejected() {
        assertThrows(IllegalArgumentException.class, () -> new UpdateAttribute("", "v"));
        assertThrows(IllegalArgumentException.class, () -> new UpdateAttribute(null, "v"));
    }

    // --- LogAttribute ---

    @Test
    void logAttributePassesThroughUnchanged() {
        var ff = FlowFile.Companion.create(new byte[]{1, 2, 3}, Map.of("k", "v"));
        var out = singleOut(new LogAttribute("[t]").process(ff));
        assertSame(ff, out, "LogAttribute must be a pass-through — same FlowFile reference");
    }

    // --- RouteOnAttribute ---

    @Test
    void routeOnAttributeMatchesFirstRule() {
        var proc = new RouteOnAttribute("high: priority == urgent; low: priority == normal");
        var ff = FlowFile.Companion.create(new byte[0], Map.of("priority", "urgent"));
        assertEquals("high", routeOf(proc.process(ff)));
    }

    @Test
    void routeOnAttributeMatchesSecondRule() {
        var proc = new RouteOnAttribute("high: priority == urgent; low: priority == normal");
        var ff = FlowFile.Companion.create(new byte[0], Map.of("priority", "normal"));
        assertEquals("low", routeOf(proc.process(ff)));
    }

    @Test
    void routeOnAttributeFallsBackToUnmatched() {
        var proc = new RouteOnAttribute("high: priority == urgent");
        var ff = FlowFile.Companion.create(new byte[0], Map.of("priority", "bogus"));
        assertEquals("unmatched", routeOf(proc.process(ff)));
    }

    @Test
    void routeOnAttributeNeqOperator() {
        var proc = new RouteOnAttribute("errors: status != ok");
        var okFf = FlowFile.Companion.create(new byte[0], Map.of("status", "ok"));
        var badFf = FlowFile.Companion.create(new byte[0], Map.of("status", "fail"));
        assertEquals("unmatched", routeOf(proc.process(okFf)));
        assertEquals("errors", routeOf(proc.process(badFf)));
    }

    @Test
    void routeOnAttributeBlankSpecYieldsUnmatched() {
        var proc = new RouteOnAttribute("");
        var ff = FlowFile.Companion.create(new byte[0], Map.of("x", "y"));
        assertEquals("unmatched", routeOf(proc.process(ff)));
    }

    @Test
    void routeOnAttributeMalformedRuleRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new RouteOnAttribute("missingcolon"));
        assertThrows(IllegalArgumentException.class,
                () -> new RouteOnAttribute("r: attr_only"));
        assertThrows(IllegalArgumentException.class,
                () -> new RouteOnAttribute("r: attr BADOP value"));
    }

    // --- FilterAttribute ---

    @Test
    void filterAttributeRemoveModeStripsListedAttributes() {
        var proc = new FilterAttribute("remove", "debug;trace");
        var ff = FlowFile.Companion.create(new byte[0], Map.of(
                "keep1", "a", "debug", "x", "keep2", "b", "trace", "y"));
        var out = singleOut(proc.process(ff));
        assert out != null;
        assertEquals(Map.of("keep1", "a", "keep2", "b"), out.getAttributes());
    }

    @Test
    void filterAttributeKeepModeRetainsOnlyListedAttributes() {
        var proc = new FilterAttribute("keep", "a;b");
        var ff = FlowFile.Companion.create(new byte[0], Map.of(
                "a", "1", "b", "2", "c", "3", "d", "4"));
        var out = singleOut(proc.process(ff));
        assert out != null;
        assertEquals(Map.of("a", "1", "b", "2"), out.getAttributes());
    }

    @Test
    void filterAttributeDefaultsToRemoveMode() {
        // mode="" / null / anything-but-"keep" behaves as remove
        var proc = new FilterAttribute("", "gone");
        var ff = FlowFile.Companion.create(new byte[0], Map.of("gone", "x", "kept", "y"));
        var out = singleOut(proc.process(ff));
        assert out != null;
        assertEquals(Map.of("kept", "y"), out.getAttributes());
    }

    @Test
    void filterAttributeEmptyListIsNoOpInRemoveMode() {
        var proc = new FilterAttribute("remove", "");
        var ff = FlowFile.Companion.create(new byte[0], Map.of("a", "1", "b", "2"));
        var out = singleOut(proc.process(ff));
        assert out != null;
        assertEquals(Map.of("a", "1", "b", "2"), out.getAttributes());
    }

    @Test
    void filterAttributeEmptyListInKeepModeDropsEverything() {
        var proc = new FilterAttribute("keep", "");
        var ff = FlowFile.Companion.create(new byte[0], Map.of("a", "1", "b", "2"));
        var out = singleOut(proc.process(ff));
        assert out != null;
        assertTrue(out.getAttributes().isEmpty());
    }

    // --- ReplaceText ---

    @Test
    void replaceTextRewritesPayload() {
        var proc = new ReplaceText("world", "Java");
        var ff = FlowFile.Companion.create("hello world".getBytes(), Map.of());
        var out = singleOut(proc.process(ff));
        assert out != null;
        if (out.getContent() instanceof RawContent rc) {
            assertEquals("hello Java", new String(rc.getBytes()));
        } else {
            fail("expected RawContent, got " + out.getContent());
        }
    }

    @Test
    void replaceTextSupportsRegexBackRefs() {
        var proc = new ReplaceText("(\\w+)@(\\w+)", "$2<-$1");
        var ff = FlowFile.Companion.create("alice@example".getBytes(), Map.of());
        var out = singleOut(proc.process(ff));
        assert out != null;
        if (out.getContent() instanceof RawContent rc) {
            assertEquals("example<-alice", new String(rc.getBytes()));
        } else {
            fail("expected RawContent, got " + out.getContent());
        }
    }

    @Test
    void replaceTextFirstModeReplacesOnlyLeadingMatch() {
        // Default "all" hits every occurrence; "first" stops after one.
        var proc = new ReplaceText("a", "X", "first");
        var ff = FlowFile.Companion.create("banana".getBytes(), Map.of());
        var out = singleOut(proc.process(ff));
        assert out != null;
        if (out.getContent() instanceof RawContent rc) {
            assertEquals("bXnana", new String(rc.getBytes()));
        } else {
            fail("expected RawContent, got " + out.getContent());
        }
    }

    @Test
    void replaceTextDefaultsToAllMode() {
        var proc = new ReplaceText("a", "X", "all");
        var ff = FlowFile.Companion.create("banana".getBytes(), Map.of());
        var out = singleOut(proc.process(ff));
        assert out != null;
        if (out.getContent() instanceof RawContent rc) {
            assertEquals("bXnXnX", new String(rc.getBytes()));
        } else {
            fail("expected RawContent, got " + out.getContent());
        }
    }

    // --- SplitText ---

    @Test
    void splitTextFansOutMultiple() {
        var proc = new SplitText(",");
        var ff = FlowFile.Companion.create("a,b,c".getBytes(), Map.of());
        var ffs = multiOut(proc.process(ff));
        assertEquals(3, ffs.size());
        if (ffs.get(1).getContent() instanceof RawContent rc) {
            assertEquals("b", new String(rc.getBytes()));
        } else {
            fail("expected RawContent at index 1");
        }
        assertEquals("1", ffs.get(1).getAttributes().get("split.index"));
        assertEquals("3", ffs.get(1).getAttributes().get("split.count"));
    }

    @Test
    void splitTextTreatsDelimiterAsRegex() {
        // C# shape: delimiter is always a regex, no flag needed.
        var proc = new SplitText("\\s+");
        var ff = FlowFile.Companion.create("one   two\tthree".getBytes(), Map.of());
        var ffs = multiOut(proc.process(ff));
        assertEquals(3, ffs.size());
    }

    @Test
    void splitTextNoDelimiterMatchPassesThrough() {
        // When the delimiter never matches, the original FlowFile is
        // returned untouched (Single, not Multiple).
        var proc = new SplitText(",");
        var ff = FlowFile.Companion.create("no-comma-here".getBytes(), Map.of());
        var out = singleOut(proc.process(ff));
        assertSame(ff, out);
    }

    @Test
    void splitTextPrependsHeaderLinesToEachChunk() {
        // headerLines=1: first line is reused as a header on every split
        // chunk — typical CSV-shard-with-header pattern.
        var proc = new SplitText(",", 1);
        var ff = FlowFile.Companion.create("col1,col2\nrow1,row1\nrow2,row2".getBytes(), Map.of());
        var ffs = multiOut(proc.process(ff));
        // The header "col1,col2\n" prefixes every emitted chunk.
        for (var piece : ffs) {
            if (piece.getContent() instanceof RawContent rc) {
                assertTrue(new String(rc.getBytes()).startsWith("col1,col2\n"),
                        "expected header prepended, got: " + new String(rc.getBytes()));
            } else {
                fail("expected RawContent, got " + piece.getContent());
            }
        }
    }

    @Test
    void splitTextSkipsBlankParts() {
        // Consecutive delimiters produce empty parts — C# skips them.
        var proc = new SplitText(",");
        var ff = FlowFile.Companion.create("a,,b,".getBytes(), Map.of());
        var ffs = multiOut(proc.process(ff));
        assertEquals(2, ffs.size());
    }
}
