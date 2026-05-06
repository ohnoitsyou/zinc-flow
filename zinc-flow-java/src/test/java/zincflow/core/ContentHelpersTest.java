package zincflow.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class ContentHelpersTest {

    @Test
    void smallPayloadStaysInlineAsRawContent() {
        var store = new MemoryContentStore();
        Content c = ContentHelpers.maybeOffload(store, new byte[128]);
        assertInstanceOf(RawContent.class, c);
        assertEquals(0, store.size(), "small payload should not touch the store");
    }

    @Test
    void largePayloadOffloadsToStoreAsClaim() {
        var store = new MemoryContentStore();
        byte[] big = new byte[ContentHelpers.DEFAULT_CLAIM_THRESHOLD + 1];
        Content c = ContentHelpers.maybeOffload(store, big);

        if (c instanceof ClaimContent claim) {
            assertEquals(big.length, claim.size());
            assertEquals(1, store.size());
            assertArrayEquals(big, store.retrieve(claim.claimId));
        } else {
            fail("expected ClaimContent, got " + c.getClass().getSimpleName());
        }
    }

    @Test
    void nullStoreForcesInlinePath() {
        byte[] big = new byte[ContentHelpers.DEFAULT_CLAIM_THRESHOLD + 1];
        Content c = ContentHelpers.maybeOffload(null, big);
        assertInstanceOf(RawContent.class, c, "without a store, everything stays inline");
    }

    @Test
    void customThresholdRespected() {
        var store = new MemoryContentStore();
        Content c = ContentHelpers.maybeOffload(store, new byte[512], 256);
        assertInstanceOf(ClaimContent.class, c);
    }

    @Test
    void nullDataBecomesEmptyRawContent() {
        Content c = ContentHelpers.maybeOffload(null, null);
        assertInstanceOf(RawContent.class, c);
        assertEquals(0, c.size());
    }
}
