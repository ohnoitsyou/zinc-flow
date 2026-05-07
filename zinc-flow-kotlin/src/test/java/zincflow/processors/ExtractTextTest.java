package zincflow.processors;

import org.junit.jupiter.api.Test;
import zincflow.core.ClaimContent;
import zincflow.core.FlowFile;
import zincflow.core.MemoryContentStore;
import zincflow.core.ProcessorResult;
import zincflow.core.RecordContent;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class ExtractTextTest {

    private static FlowFile single(ProcessorResult result) {
        return switch (result) {
            case ProcessorResult.Single ff -> ff.flowFile;
            default -> {
                fail("expected Single, got " + result);
                yield null;
            }
        };
    }

    @Test
    void namedGroupsBecomeAttributes() {
        var proc = new ExtractText(
                "order=(?<orderId>\\d+) user=(?<user>\\w+)",
                "",
                null);
        var ff = FlowFile.Companion.create("order=42 user=alice".getBytes(StandardCharsets.UTF_8), Map.of());
        var out = single(proc.process(ff));
        assert out != null;
        assertEquals("42", out.getAttributes().get("orderId"));
        assertEquals("alice", out.getAttributes().get("user"));
    }

    @Test
    void positionalGroupsMappedViaGroupNames() {
        var proc = new ExtractText("(\\w+)@(\\w+\\.\\w+)", "localpart, domain", null);
        var ff = FlowFile.Companion.create("contact: bob@example.com".getBytes(StandardCharsets.UTF_8), Map.of());
        var out = single(proc.process(ff));
        assert out != null;
        assertEquals("bob", out.getAttributes().get("localpart"));
        assertEquals("example.com", out.getAttributes().get("domain"));
    }

    @Test
    void noMatchPassesFlowFileThroughUnchanged() {
        var proc = new ExtractText("NOMATCH-(\\d+)", "", null);
        var ff = FlowFile.Companion.create("payload".getBytes(StandardCharsets.UTF_8),
                Map.of("preserved", "yes"));
        var out = single(proc.process(ff));
        assertSame(ff, out, "no-match branch should pass the original FlowFile through unchanged");
        assert out != null;
        assertEquals("yes", out.getAttributes().get("preserved"));
    }

    @Test
    void resolvesClaimContentViaStore() {
        var store = new MemoryContentStore();
        byte[] payload = "x=7".getBytes(StandardCharsets.UTF_8);
        String claimId = store.store(payload);
        var proc = new ExtractText("x=(?<x>\\d+)", "", store);
        var ff = FlowFile.Companion.create(new ClaimContent(claimId, payload.length), Map.of());
        var out = single(proc.process(ff));
        assert out != null;
        assertEquals("7", out.getAttributes().get("x"));
    }

    @Test
    void recordContentFails() {
        var proc = new ExtractText("x=(?<x>\\d+)", "", null);
        var ff = FlowFile.Companion.create(RecordContent.Companion.invoke(List.of(Map.of("x", 7)), null), Map.of());
        assertInstanceOf(ProcessorResult.Failure.class, proc.process(ff));
    }

    @Test
    void blankPatternRejectedAtConstruction() {
        assertThrows(IllegalArgumentException.class, () -> new ExtractText("", "", null));
        // ExtractText doesn't accept a null for a regex
        assertThrows(NullPointerException.class, () -> new ExtractText(null, "", null));
    }
}
