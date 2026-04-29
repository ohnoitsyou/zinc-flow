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
            case ProcessorResult.Single(FlowFile ff) -> ff;
            default -> {
                fail("expected Single, got " + r);
                yield null;
            }
        };
    }

    private static String routeOf(ProcessorResult r) {
        return switch (r) {
            case ProcessorResult.Routed(String name, FlowFile ignored) -> name;
            default -> {
                fail("expected Routed, got " + r);
                yield "";
            }
        };
    }

    private static List<FlowFile> multiOut(ProcessorResult r) {
        return switch (r) {
            case ProcessorResult.Multiple(List<FlowFile> ffs) -> ffs;
            default -> {
                fail("expected Multiple, got " + r);
                yield List.of();
            }
        };
    }

    // --- UpdateAttribute ---

    @Test
    void updateAttributeSetsKeyAndWrapsInSingle() {
        var ff = FlowFile.create(new byte[0], Map.of());
        var out = singleOut(new UpdateAttribute("priority", "high").process(ff));
        assertEquals("high", out.attributes().get("priority"));
    }

    @Test
    void updateAttributeBlankKeyRejected() {
        assertThrows(IllegalArgumentException.class, () -> new UpdateAttribute("", "v"));
        assertThrows(IllegalArgumentException.class, () -> new UpdateAttribute(null, "v"));
    }

    // --- LogAttribute ---

    @Test
    void logAttributePassesThroughUnchanged() {
        var ff = FlowFile.create(new byte[]{1, 2, 3}, Map.of("k", "v"));
        var out = singleOut(new LogAttribute("[t]").process(ff));
        assertSame(ff, out, "LogAttribute must be a pass-through — same FlowFile reference");
    }

    // --- RouteOnAttribute ---

    @Test
    void routeOnAttributeMatchesFirstRule() {
        var proc = new RouteOnAttribute("high: priority == urgent; low: priority == normal");
        var ff = FlowFile.create(new byte[0], Map.of("priority", "urgent"));
        assertEquals("high", routeOf(proc.process(ff)));
    }

    @Test
    void routeOnAttributeMatchesSecondRule() {
        var proc = new RouteOnAttribute("high: priority == urgent; low: priority == normal");
        var ff = FlowFile.create(new byte[0], Map.of("priority", "normal"));
        assertEquals("low", routeOf(proc.process(ff)));
    }

    @Test
    void routeOnAttributeFallsBackToUnmatched() {
        var proc = new RouteOnAttribute("high: priority == urgent");
        var ff = FlowFile.create(new byte[0], Map.of("priority", "bogus"));
        assertEquals("unmatched", routeOf(proc.process(ff)));
    }

    @Test
    void routeOnAttributeNeqOperator() {
        var proc = new RouteOnAttribute("errors: status != ok");
        var okFf = FlowFile.create(new byte[0], Map.of("status", "ok"));
        var badFf = FlowFile.create(new byte[0], Map.of("status", "fail"));
        assertEquals("unmatched", routeOf(proc.process(okFf)));
        assertEquals("errors", routeOf(proc.process(badFf)));
    }

    @Test
    void routeOnAttributeBlankSpecYieldsUnmatched() {
        var proc = new RouteOnAttribute("");
        var ff = FlowFile.create(new byte[0], Map.of("x", "y"));
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
        var ff = FlowFile.create(new byte[0], Map.of(
                "keep1", "a", "debug", "x", "keep2", "b", "trace", "y"));
        var out = singleOut(proc.process(ff));
        assertEquals(Map.of("keep1", "a", "keep2", "b"), out.attributes());
    }

    @Test
    void filterAttributeKeepModeRetainsOnlyListedAttributes() {
        var proc = new FilterAttribute("keep", "a;b");
        var ff = FlowFile.create(new byte[0], Map.of(
                "a", "1", "b", "2", "c", "3", "d", "4"));
        var out = singleOut(proc.process(ff));
        assertEquals(Map.of("a", "1", "b", "2"), out.attributes());
    }

    @Test
    void filterAttributeDefaultsToRemoveMode() {
        // mode="" / null / anything-but-"keep" behaves as remove
        var proc = new FilterAttribute("", "gone");
        var ff = FlowFile.create(new byte[0], Map.of("gone", "x", "kept", "y"));
        var out = singleOut(proc.process(ff));
        assertEquals(Map.of("kept", "y"), out.attributes());
    }

    @Test
    void filterAttributeEmptyListIsNoOpInRemoveMode() {
        var proc = new FilterAttribute("remove", "");
        var ff = FlowFile.create(new byte[0], Map.of("a", "1", "b", "2"));
        var out = singleOut(proc.process(ff));
        assertEquals(Map.of("a", "1", "b", "2"), out.attributes());
    }

    @Test
    void filterAttributeEmptyListInKeepModeDropsEverything() {
        var proc = new FilterAttribute("keep", "");
        var ff = FlowFile.create(new byte[0], Map.of("a", "1", "b", "2"));
        var out = singleOut(proc.process(ff));
        assertTrue(out.attributes().isEmpty());
    }

    // --- ReplaceText ---

    @Test
    void replaceTextRewritesPayload() {
        var proc = new ReplaceText("world", "Java");
        var ff = FlowFile.create("hello world".getBytes(), Map.of());
        var out = singleOut(proc.process(ff));
        if (out.content instanceof RawContent(byte[] bytes)) {
            assertEquals("hello Java", new String(bytes));
        } else {
            fail("expected RawContent, got " + out.content);
        }
    }

    @Test
    void replaceTextSupportsRegexBackRefs() {
        var proc = new ReplaceText("(\\w+)@(\\w+)", "$2<-$1");
        var ff = FlowFile.create("alice@example".getBytes(), Map.of());
        var out = singleOut(proc.process(ff));
        if (out.content instanceof RawContent(byte[] bytes)) {
            assertEquals("example<-alice", new String(bytes));
        } else {
            fail("expected RawContent, got " + out.content);
        }
    }

    @Test
    void replaceTextFirstModeReplacesOnlyLeadingMatch() {
        // Default "all" hits every occurrence; "first" stops after one.
        var proc = new ReplaceText("a", "X", "first");
        var ff = FlowFile.create("banana".getBytes(), Map.of());
        var out = singleOut(proc.process(ff));
        if (out.content instanceof RawContent(byte[] bytes)) {
            assertEquals("bXnana", new String(bytes));
        } else {
            fail("expected RawContent, got " + out.content);
        }
    }

    @Test
    void replaceTextDefaultsToAllMode() {
        var proc = new ReplaceText("a", "X", "all");
        var ff = FlowFile.create("banana".getBytes(), Map.of());
        var out = singleOut(proc.process(ff));
        if (out.content instanceof RawContent(byte[] bytes)) {
            assertEquals("bXnXnX", new String(bytes));
        } else {
            fail("expected RawContent, got " + out.content);
        }
    }

    // --- SplitText ---

    @Test
    void splitTextFansOutMultiple() {
        var proc = new SplitText(",");
        var ff = FlowFile.create("a,b,c".getBytes(), Map.of());
        var ffs = multiOut(proc.process(ff));
        assertEquals(3, ffs.size());
        if (ffs.get(1).content instanceof RawContent(byte[] bytes)) {
            assertEquals("b", new String(bytes));
        } else {
            fail("expected RawContent at index 1");
        }
        assertEquals("1", ffs.get(1).attributes().get("split.index"));
        assertEquals("3", ffs.get(1).attributes().get("split.count"));
    }

    @Test
    void splitTextTreatsDelimiterAsRegex() {
        // C# shape: delimiter is always a regex, no flag needed.
        var proc = new SplitText("\\s+");
        var ff = FlowFile.create("one   two\tthree".getBytes(), Map.of());
        var ffs = multiOut(proc.process(ff));
        assertEquals(3, ffs.size());
    }

    @Test
    void splitTextNoDelimiterMatchPassesThrough() {
        // When the delimiter never matches, the original FlowFile is
        // returned untouched (Single, not Multiple).
        var proc = new SplitText(",");
        var ff = FlowFile.create("no-comma-here".getBytes(), Map.of());
        var out = singleOut(proc.process(ff));
        assertSame(ff, out);
    }

    @Test
    void splitTextPrependsHeaderLinesToEachChunk() {
        // headerLines=1: first line is reused as a header on every split
        // chunk — typical CSV-shard-with-header pattern.
        var proc = new SplitText(",", 1);
        var ff = FlowFile.create("col1,col2\nrow1,row1\nrow2,row2".getBytes(), Map.of());
        var ffs = multiOut(proc.process(ff));
        // The header "col1,col2\n" prefixes every emitted chunk.
        for (var piece : ffs) {
            if (piece.content instanceof RawContent(byte[] bytes)) {
                assertTrue(new String(bytes).startsWith("col1,col2\n"),
                        "expected header prepended, got: " + new String(bytes));
            } else {
                fail("expected RawContent, got " + piece.content);
            }
        }
    }

    @Test
    void splitTextSkipsBlankParts() {
        // Consecutive delimiters produce empty parts — C# skips them.
        var proc = new SplitText(",");
        var ff = FlowFile.create("a,,b,".getBytes(), Map.of());
        var ffs = multiOut(proc.process(ff));
        assertEquals(2, ffs.size());
    }
}
