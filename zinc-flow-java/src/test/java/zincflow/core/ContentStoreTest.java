package zincflow.core;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class ContentStoreTest {

    @Test
    void memoryStoreRoundTrip() {
        var store = new MemoryContentStore();
        byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
        String id = store.store(data);

        assertTrue(store.exists(id));
        assertArrayEquals(data, store.retrieve(id));

        store.delete(id);
        assertFalse(store.exists(id));
        assertEquals(0, store.retrieve(id).length);
    }

    @Test
    void memoryStoreAssignsDistinctIds() {
        var store = new MemoryContentStore();
        String a = store.store("a".getBytes());
        String b = store.store("b".getBytes());
        assertNotEquals(a, b);
        assertArrayEquals("a".getBytes(), store.retrieve(a));
        assertArrayEquals("b".getBytes(), store.retrieve(b));
    }

    @Test
    void claimContentHoldsMetadata() {
        var claim = new ClaimContent("mem-claim-7", 1024);
        assertEquals("mem-claim-7", claim.claimId);
        assertEquals(1024, claim.size);
    }

    @Test
    void claimContentRejectsBlankIdAndNegativeSize() {
        assertThrows(IllegalArgumentException.class, () -> new ClaimContent("", 1));
        assertThrows(IllegalArgumentException.class, () -> new ClaimContent(null, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClaimContent("id", -1));
    }

    @Test
    void contentResolverReturnsBytesForRawContent() {
        byte[] data = "raw".getBytes();
        var res = ContentResolver.resolve(new RawContent(data), null);
        assertTrue(res.ok());
        assertArrayEquals(data, res.bytes);
    }

    @Test
    void contentResolverFetchesClaimViaStore() {
        var store = new MemoryContentStore();
        byte[] data = "stored".getBytes();
        String id = store.store(data);
        var res = ContentResolver.resolve(new ClaimContent(id, data.length), store);
        assertTrue(res.ok());
        assertArrayEquals(data, res.bytes);
    }

    @Test
    void contentResolverErrorsOnRecordContentAndMissingStore() {
        var recs = RecordContent.Companion.invoke(List.of(Map.of("k", "v")), null);
        assertFalse(ContentResolver.resolve(recs, new MemoryContentStore()).ok());
        assertFalse(ContentResolver.resolve(new ClaimContent("x", 0), null).ok());
    }
}
