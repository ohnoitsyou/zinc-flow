package zincflow.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

final class FileContentStoreTest {

    @Test
    void roundTripPreservesBytes(@TempDir Path dir) throws Exception {
        var store = new FileContentStore(dir);
        byte[] data = "payload bytes".getBytes(StandardCharsets.UTF_8);
        String id = store.store(data);

        assertTrue(store.exists(id));
        assertEquals(data.length, store.size(id));
        assertArrayEquals(data, store.retrieve(id));
    }

    @Test
    void streamStoreAndOpenReadBypassHeap(@TempDir Path dir) throws Exception {
        var store = new FileContentStore(dir);
        byte[] data = "streamed payload".getBytes(StandardCharsets.UTF_8);

        String id;
        try (InputStream in = new ByteArrayInputStream(data)) {
            id = store.store(in);
        }

        try (InputStream in = store.openRead(id)) {
            assertArrayEquals(data, in.readAllBytes());
        }
    }

    @Test
    void shardedDirectoryLayoutKeepsLargeIdSetsManageable(@TempDir Path dir) throws Exception {
        var store = new FileContentStore(dir);
        String id = store.store("x".getBytes());
        String shard = id.substring(0, 2);
        Path shardDir = dir.resolve(shard);
        assertTrue(java.nio.file.Files.isDirectory(shardDir),
                "expected shard directory " + shardDir);
        assertTrue(java.nio.file.Files.isRegularFile(shardDir.resolve(id)));
    }

    @Test
    void deleteRemovesClaim(@TempDir Path dir) throws Exception {
        var store = new FileContentStore(dir);
        String id = store.store("gone".getBytes());
        assertTrue(store.exists(id));
        store.delete(id);
        assertFalse(store.exists(id));
        assertEquals(0, store.retrieve(id).length);
    }

    @Test
    void uniqueIdsAcrossManyStores(@TempDir Path dir) throws Exception {
        var store = new FileContentStore(dir);
        var ids = new java.util.HashSet<String>();
        for (int i = 0; i < 100; i++) ids.add(store.store(new byte[] { (byte) i }));
        assertEquals(100, ids.size());
    }
}
