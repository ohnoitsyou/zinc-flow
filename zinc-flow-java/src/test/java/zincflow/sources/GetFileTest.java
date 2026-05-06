package zincflow.sources;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import zincflow.core.FlowFile;
import zincflow.core.RawContent;
import zincflow.fabric.FlowFileV3;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

final class GetFileTest {

    @Test
    void scansDirectoryAndEmitsRegularFile(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("hello.txt"), "hi there");
        var src = new GetFile("file", dir, "*", 1000, true);

        List<FlowFile> batch = src.poll();

        assertEquals(1, batch.size());
        FlowFile ff = batch.getFirst();
        assertEquals("hello.txt", ff.getAttributes().get("filename"));
        assertEquals(dir.resolve("hello.txt").toAbsolutePath().toString(),
                ff.getAttributes().get("path"));
        assertEquals("file", ff.getAttributes().get("source"));
        assertEquals("8", ff.getAttributes().get("size"));
        assertArrayEquals("hi there".getBytes(StandardCharsets.UTF_8),
                ((RawContent) ff.getContent()).getBytes());
    }

    @Test
    void unpacksV3BundleIntoMultipleFlowFiles(@TempDir Path dir) throws Exception {
        FlowFile ff1 = FlowFile.Companion.create("a".getBytes(), Map.of("k1", "v1"));
        FlowFile ff2 = FlowFile.Companion.create("bb".getBytes(), Map.of("k2", "v2"));
        byte[] bundle = FlowFileV3.packMultiple(List.of(ff1, ff2),
                List.of("a".getBytes(), "bb".getBytes()));
        Files.write(dir.resolve("bundle.bin"), bundle);

        var src = new GetFile("file", dir, "*", 1000, true);
        List<FlowFile> batch = src.poll();

        assertEquals(2, batch.size());
        assertEquals("v1", batch.get(0).getAttributes().get("k1"));
        assertEquals("v2", batch.get(1).getAttributes().get("k2"));
        assertEquals("0", batch.get(0).getAttributes().get("v3.frame.index"));
        assertEquals("2", batch.get(0).getAttributes().get("v3.frame.count"));
        assertEquals("bundle.bin", batch.getFirst().getAttributes().get("filename"));
    }

    @Test
    void acceptedFilesMoveToProcessedOnlyAfterAllFramesIngested(@TempDir Path dir) throws Exception {
        FlowFile a = FlowFile.Companion.create("a".getBytes(), Map.of());
        FlowFile b = FlowFile.Companion.create("b".getBytes(), Map.of());
        byte[] bundle = FlowFileV3.packMultiple(List.of(a, b),
                List.of("a".getBytes(), "b".getBytes()));
        Path src = dir.resolve("two.bin");
        Files.write(src, bundle);

        var source = new GetFile("file", dir, "*", 10, true);
        CountDownLatch done = new CountDownLatch(2);
        List<FlowFile> delivered = new CopyOnWriteArrayList<>();
        source.start(ff -> { delivered.add(ff); done.countDown(); return true; });
        assertTrue(done.await(2, TimeUnit.SECONDS));
        // Give onIngested a beat to land + move.
        for (int i = 0; i < 50 && Files.exists(src); i++) Thread.sleep(20);
        source.stop();

        assertFalse(Files.exists(src), "source file should have moved after both frames landed");
        assertTrue(Files.exists(dir.resolve(".processed/two.bin")));
    }

    @Test
    void rejectedFileStaysInInputDir(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("keep.txt");
        Files.writeString(src, "not consumed");
        var source = new GetFile("file", dir, "*", 10, true);

        CountDownLatch rejected = new CountDownLatch(1);
        source.start(ff -> { rejected.countDown(); return false; });
        assertTrue(rejected.await(2, TimeUnit.SECONDS));
        source.stop();

        assertTrue(Files.exists(src), "rejected file must remain for the next poll");
        assertFalse(Files.exists(dir.resolve(".processed/keep.txt")));
    }

    @Test
    void patternFiltersFilenames(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("a.json"), "{}");
        Files.writeString(dir.resolve("b.txt"), "x");
        var src = new GetFile("file", dir, "*.json", 1000, true);
        List<FlowFile> batch = src.poll();
        assertEquals(1, batch.size());
        assertEquals("a.json", batch.getFirst().getAttributes().get("filename"));
    }
}
