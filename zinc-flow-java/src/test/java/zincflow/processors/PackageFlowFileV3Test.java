package zincflow.processors;

import org.junit.jupiter.api.Test;
import zincflow.core.ClaimContent;
import zincflow.core.FlowFile;
import zincflow.core.MemoryContentStore;
import zincflow.core.ProcessorResult;
import zincflow.core.RawContent;
import zincflow.core.RecordContent;
import zincflow.fabric.FlowFileV3;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class PackageFlowFileV3Test {

    @Test
    void packUnpackRoundTripRestoresAttributesAndContent() {
        var ff = FlowFile.create("the payload".getBytes(StandardCharsets.UTF_8),
                Map.of("tenant", "acme", "filename", "doc.txt"));

        if (!(new PackageFlowFileV3().process(ff) instanceof ProcessorResult.Single(FlowFile packed))) {
            fail("pack: expected Single");
            return;
        }
        assertEquals("application/flowfile-v3", packed.attributes().get("http.content.type"));
        assertEquals("true", packed.attributes().get("v3.packaged"));
        assertInstanceOf(RawContent.class, packed.content);

        if (!(new UnpackageFlowFileV3().process(packed) instanceof ProcessorResult.Single(FlowFile restored))) {
            fail("unpack: expected Single");
            return;
        }
        assertEquals("acme", restored.attributes().get("tenant"));
        assertEquals("doc.txt", restored.attributes().get("filename"));
        if (restored.content instanceof RawContent(byte[] bytes)) {
            assertEquals("the payload", new String(bytes, StandardCharsets.UTF_8));
        } else {
            fail("expected RawContent after unpack, got " + restored.content.getClass().getSimpleName());
        }
    }

    @Test
    void packageRejectsRecordContent() {
        var ff = FlowFile.create(new RecordContent(List.of(Map.of("k", 1))), Map.of());
        assertInstanceOf(ProcessorResult.Failure.class, new PackageFlowFileV3().process(ff));
    }

    @Test
    void unpackageFailsOnNonV3Input() {
        var ff = FlowFile.create("not v3 framed".getBytes(), Map.of());
        var result = new UnpackageFlowFileV3().process(ff);
        if (result instanceof ProcessorResult.Failure(String reason, FlowFile ignored)) {
            assertTrue(reason.toLowerCase().contains("magic"),
                    "expected magic-related failure, got: " + reason);
        } else {
            fail("expected Failure, got " + result);
        }
    }

    @Test
    void unpackageEmitsMultipleWhenStreamContainsSeveral() {
        var a = FlowFile.create("A".getBytes(), Map.of("i", "0"));
        var b = FlowFile.create("BB".getBytes(), Map.of("i", "1"));
        byte[] packed = FlowFileV3.packMultiple(
                List.of(a, b),
                List.of("A".getBytes(), "BB".getBytes()));
        var combined = FlowFile.create(packed, Map.of());

        var result = new UnpackageFlowFileV3().process(combined);
        if (result instanceof ProcessorResult.Multiple(List<FlowFile> ffs)) {
            assertEquals(2, ffs.size());
            assertEquals("0", ffs.get(0).attributes().get("i"));
            assertEquals("1", ffs.get(1).attributes().get("i"));
        } else {
            fail("expected Multiple, got " + result);
        }
    }

    @Test
    void packageResolvesClaimContentViaStore() {
        var store = new MemoryContentStore();
        byte[] payload = "stored".getBytes(StandardCharsets.UTF_8);
        String id = store.store(payload);
        var ff = FlowFile.create(new ClaimContent(id, payload.length), Map.of("k", "v"));

        if (!(new PackageFlowFileV3(store).process(ff) instanceof ProcessorResult.Single(FlowFile packed))) {
            fail("expected Single from pack");
            return;
        }
        byte[] packedBytes = switch (packed.content) {
            case RawContent raw -> raw.bytes;
            default -> { fail("pack output should be raw"); yield new byte[0]; }
        };

        var restored = FlowFileV3.unpack(packedBytes, 0).flowFile;
        assertEquals("v", restored.attributes().get("k"));
        if (restored.content instanceof RawContent(byte[] out)) {
            assertEquals("stored", new String(out, StandardCharsets.UTF_8));
        } else {
            fail("expected RawContent, got " + restored.content.getClass().getSimpleName());
        }
    }
}
