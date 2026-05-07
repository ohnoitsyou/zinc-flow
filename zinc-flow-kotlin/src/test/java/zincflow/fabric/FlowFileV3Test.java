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
        var ff = FlowFile.Companion.create("hello, world".getBytes(StandardCharsets.UTF_8), Map.of(
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
        assert restored != null;
        assertEquals("payload.txt", restored.getAttributes().get("filename"));
        assertEquals("acme", restored.getAttributes().get("tenant"));
        assertEquals("hello, world",
                new String(((zincflow.core.RawContent) restored.getContent()).getBytes(), StandardCharsets.UTF_8));
    }

    @Test
    void multipleFlowFilesPackedAndUnpacked() {
        var a = FlowFile.Companion.create("A".getBytes(), Map.of("idx", "0"));
        var b = FlowFile.Companion.create("BB".getBytes(), Map.of("idx", "1"));
        var c = FlowFile.Companion.create("CCC".getBytes(), Map.of("idx", "2"));
        byte[] packed = FlowFileV3.packMultiple(
                List.of(a, b, c),
                List.of("A".getBytes(), "BB".getBytes(), "CCC".getBytes()));

        var unpacked = FlowFileV3.unpackAll(packed);
        assertEquals(3, unpacked.size());
        assertEquals("0", unpacked.get(0).getAttributes().get("idx"));
        assertEquals("1", unpacked.get(1).getAttributes().get("idx"));
        assertEquals("2", unpacked.get(2).getAttributes().get("idx"));
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
        var ff = FlowFile.Companion.create(new byte[0], Map.of("k", "v"));
        byte[] packed = FlowFileV3.pack(ff, new byte[0]);
        var result = FlowFileV3.unpack(packed, 0);
        assertTrue(result.ok());
        assert result.flowFile != null;
        assertEquals("v", result.flowFile.getAttributes().get("k"));
        assertEquals(0, result.flowFile.getContent().size());
    }

    @Test
    void largeAttributeValueTakesExtendedLengthEncoding() {
        // Values at or above 0xFFFF bytes should trigger the 6-byte length encoding.
        String big = "x".repeat(70_000);
        var ff = FlowFile.Companion.create("p".getBytes(), Map.of("big", big));
        byte[] packed = FlowFileV3.pack(ff, "p".getBytes());
        var restored = FlowFileV3.unpack(packed, 0).flowFile;
        assert restored != null;
        assertEquals(big, restored.getAttributes().get("big"));
    }

    @Test
    void truncatedBufferReportsError() {
        var ff = FlowFile.Companion.create("abc".getBytes(), Map.of("k", "v"));
        byte[] packed = FlowFileV3.pack(ff, "abc".getBytes());
        byte[] truncated = java.util.Arrays.copyOf(packed, packed.length - 2);
        var result = FlowFileV3.unpack(truncated, 0);
        assertFalse(result.ok());
    }
}
