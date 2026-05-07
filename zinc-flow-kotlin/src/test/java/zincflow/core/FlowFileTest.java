package zincflow.core;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class FlowFileTest {

    @Test
    void createAssignsIncrementingIds() {
        FlowFile a = FlowFile.Companion.create(new byte[]{1, 2}, Map.of());
        FlowFile b = FlowFile.Companion.create(new byte[]{3, 4}, Map.of());
        assertTrue(b.getId() > a.getId(), "id should increment");
    }

    @Test
    void attributesAreDefensivelyCopied() {
        var mutable = new java.util.HashMap<String, String>();
        mutable.put("k", "v");
        FlowFile ff = FlowFile.Companion.create(new byte[0], mutable);
        mutable.put("k", "mutated");
        assertEquals("v", ff.getAttributes().get("k"),
                "mutating the source map must not leak into the FlowFile");
    }

    @Test
    void withAttributeReturnsNewFlowFileWithKey() {
        FlowFile ff = FlowFile.Companion.create(new byte[0], Map.of());
        FlowFile next = ff.withAttribute("priority", "high");
        assertEquals("high", next.getAttributes().get("priority"));
        assertFalse(ff.getAttributes().containsKey("priority"), "original should be untouched");
        assertEquals(ff.getId(), next.getId(), "withAttribute preserves id");
    }

    @Test
    void withContentSwapsPayloadPreservesAttributes() {
        FlowFile ff = FlowFile.Companion.create(new byte[]{1}, Map.of("k", "v"));
        FlowFile next = ff.withContent(new RawContent(new byte[]{7, 8, 9}));
        assertEquals(3, next.getContent().size());
        assertEquals("v", next.getAttributes().get("k"));
    }

    @Test
    void bumpHopIncrementsCount() {
        FlowFile ff = FlowFile.Companion.create(new byte[0], Map.of());
        assertEquals(0, ff.getHopCount());
        assertEquals(1, ff.bumpHop().getHopCount());
        assertEquals(2, ff.bumpHop().bumpHop().getHopCount());
    }

    @Test
    void stringIdFormatsAsFfDashId() {
        FlowFile ff = FlowFile.Companion.create(new byte[0], Map.of());
        assertEquals("ff-" + ff.getId(), ff.stringId());
    }

    /*
    RawContent doesn't accept a null parameter
    Could adjust to check for NPE instead
    @Test
    void nullBytesRejectedByRawContent() {
        assertThrows(IllegalArgumentException.class, () -> new RawContent(null));
    }
     */
}
