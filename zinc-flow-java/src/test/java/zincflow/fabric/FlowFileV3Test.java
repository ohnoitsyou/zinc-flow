package zincflow.fabric;

import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class FlowFileV3Test {

    @Test
    void roundTripPreservesAttributesAndContent() {
        var ff = FlowFile.create("hello, world".getBytes(StandardCharsets.UTF_8), Map.of(
                "filename", "payload.txt",
                "tenant", "acme"));
        byte[] packed = FlowFileV3.pack(ff, "hello, world".getBytes(StandardCharsets.UTF_8));

        // Starts with the magic
        assertArrayEquals(FlowFileV3.MAGIC,
                java.util.Arrays.copyOfRange(packed, 0, FlowFileV3.MAGIC_LEN));

        var result = FlowFileV3.unpack(packed, 0);
        assertTrue(result.ok());
        assertEquals(packed.length, result.nextOffset);

        var restored = result.flowFile;
        assertEquals("payload.txt", restored.attributes().get("filename"));
        assertEquals("acme", restored.attributes().get("tenant"));
        assertEquals("hello, world",
                new String(((zincflow.core.RawContent) restored.content).bytes, StandardCharsets.UTF_8));
    }

    @Test
    void multipleFlowFilesPackedAndUnpacked() {
        var a = FlowFile.create("A".getBytes(), Map.of("idx", "0"));
        var b = FlowFile.create("BB".getBytes(), Map.of("idx", "1"));
        var c = FlowFile.create("CCC".getBytes(), Map.of("idx", "2"));
        byte[] packed = FlowFileV3.packMultiple(
                List.of(a, b, c),
                List.of("A".getBytes(), "BB".getBytes(), "CCC".getBytes()));

        var unpacked = FlowFileV3.unpackAll(packed);
        assertEquals(3, unpacked.size());
        assertEquals("0", unpacked.get(0).attributes().get("idx"));
        assertEquals("1", unpacked.get(1).attributes().get("idx"));
        assertEquals("2", unpacked.get(2).attributes().get("idx"));
    }

    @Test
    void missingMagicReturnsError() {
        byte[] garbage = "not a V3 payload".getBytes();
        var result = FlowFileV3.unpack(garbage, 0);
        assertFalse(result.ok());
        assertTrue(result.error.toLowerCase().contains("magic"));
    }

    @Test
    void emptyContentRoundTrips() {
        var ff = FlowFile.create(new byte[0], Map.of("k", "v"));
        byte[] packed = FlowFileV3.pack(ff, new byte[0]);
        var result = FlowFileV3.unpack(packed, 0);
        assertTrue(result.ok());
        assertEquals("v", result.flowFile.attributes().get("k"));
        assertEquals(0, result.flowFile.content.size());
    }

    @Test
    void largeAttributeValueTakesExtendedLengthEncoding() {
        // Values at or above 0xFFFF bytes should trigger the 6-byte length encoding.
        String big = "x".repeat(70_000);
        var ff = FlowFile.create("p".getBytes(), Map.of("big", big));
        byte[] packed = FlowFileV3.pack(ff, "p".getBytes());
        var restored = FlowFileV3.unpack(packed, 0).flowFile;
        assertEquals(big, restored.attributes().get("big"));
    }

    @Test
    void truncatedBufferReportsError() {
        var ff = FlowFile.create("abc".getBytes(), Map.of("k", "v"));
        byte[] packed = FlowFileV3.pack(ff, "abc".getBytes());
        byte[] truncated = java.util.Arrays.copyOf(packed, packed.length - 2);
        var result = FlowFileV3.unpack(truncated, 0);
        assertFalse(result.ok());
    }
}
