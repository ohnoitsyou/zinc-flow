package zincflow.core;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class FlowFileTest {

    @Test
    void createAssignsIncrementingIds() {
        FlowFile a = FlowFile.create(new byte[]{1, 2}, Map.of());
        FlowFile b = FlowFile.create(new byte[]{3, 4}, Map.of());
        assertTrue(b.id > a.id, "id should increment");
    }

    @Test
    void attributesAreDefensivelyCopied() {
        var mutable = new java.util.HashMap<String, String>();
        mutable.put("k", "v");
        FlowFile ff = FlowFile.create(new byte[0], mutable);
        mutable.put("k", "mutated");
        assertEquals("v", ff.attributes().get("k"),
                "mutating the source map must not leak into the FlowFile");
    }

    @Test
    void withAttributeReturnsNewFlowFileWithKey() {
        FlowFile ff = FlowFile.create(new byte[0], Map.of());
        FlowFile next = ff.withAttribute("priority", "high");
        assertEquals("high", next.attributes().get("priority"));
        assertFalse(ff.attributes().containsKey("priority"), "original should be untouched");
        assertEquals(ff.id, next.id, "withAttribute preserves id");
    }

    @Test
    void withContentSwapsPayloadPreservesAttributes() {
        FlowFile ff = FlowFile.create(new byte[]{1}, Map.of("k", "v"));
        FlowFile next = ff.withContent(new RawContent(new byte[]{7, 8, 9}));
        assertEquals(3, next.content.size());
        assertEquals("v", next.attributes().get("k"));
    }

    @Test
    void bumpHopIncrementsCount() {
        FlowFile ff = FlowFile.create(new byte[0], Map.of());
        assertEquals(0, ff.hopCount);
        assertEquals(1, ff.bumpHop().hopCount);
        assertEquals(2, ff.bumpHop().bumpHop().hopCount);
    }

    @Test
    void stringIdFormatsAsFfDashId() {
        FlowFile ff = FlowFile.create(new byte[0], Map.of());
        assertEquals("ff-" + ff.id, ff.stringId());
    }

    @Test
    void nullBytesRejectedByRawContent() {
        assertThrows(IllegalArgumentException.class, () -> new RawContent(null));
    }
}
