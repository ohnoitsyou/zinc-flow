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
        var ff = FlowFile.Companion.create("the payload".getBytes(StandardCharsets.UTF_8),
                Map.of("tenant", "acme", "filename", "doc.txt"));

        if (!(new PackageFlowFileV3().process(ff) instanceof ProcessorResult.Single single)) {
            fail("pack: expected Single");
            return;
        }
        assertEquals("application/flowfile-v3", single.flowFile.getAttributes().get("http.content.type"));
        assertEquals("true", single.flowFile.getAttributes().get("v3.packaged"));
        assertInstanceOf(RawContent.class, single.flowFile.getContent());

        if (!(new UnpackageFlowFileV3().process(single.flowFile) instanceof ProcessorResult.Single singleRestored )) {
            fail("unpack: expected Single");
            return;
        }
        assertEquals("acme", singleRestored.flowFile.getAttributes().get("tenant"));
        assertEquals("doc.txt", singleRestored.flowFile.getAttributes().get("filename"));
        if (singleRestored.flowFile.getContent() instanceof RawContent rc) {
            assertEquals("the payload", new String(rc.getBytes(), StandardCharsets.UTF_8));
        } else {
            fail("expected RawContent after unpack, got " + singleRestored.flowFile.getContent().getClass().getSimpleName());
        }
    }

    @Test
    void packageRejectsRecordContent() {
        var ff = FlowFile.Companion.create(RecordContent.Companion.invoke(List.of(Map.of("k", 1)), null), Map.of());
        assertInstanceOf(ProcessorResult.Failure.class, new PackageFlowFileV3().process(ff));
    }

    @Test
    void unpackageFailsOnNonV3Input() {
        var ff = FlowFile.Companion.create("not v3 framed".getBytes(), Map.of());
        var result = new UnpackageFlowFileV3().process(ff);
        if (result instanceof ProcessorResult.Failure failure) {// (String reason, FlowFile ignored)) {
            assertTrue(failure.getReason().toLowerCase().contains("magic"),
                    "expected magic-related failure, got: " + failure.getReason());
        } else {
            fail("expected Failure, got " + result);
        }
    }

    @Test
    void unpackageEmitsMultipleWhenStreamContainsSeveral() {
        var a = FlowFile.Companion.create("A".getBytes(), Map.of("i", "0"));
        var b = FlowFile.Companion.create("BB".getBytes(), Map.of("i", "1"));
        byte[] packed = FlowFileV3.packMultiple(
                List.of(a, b),
                List.of("A".getBytes(), "BB".getBytes()));
        var combined = FlowFile.Companion.create(packed, Map.of());

        var result = new UnpackageFlowFileV3().process(combined);
        if (result instanceof ProcessorResult.Multiple multiple) {
            assertEquals(2, multiple.getFlowFiles().size());
            assertEquals("0", multiple.getFlowFiles().getFirst().getAttributes().get("i"));
            assertEquals("1", multiple.getFlowFiles().get(1).getAttributes().get("i"));
        } else {
            fail("expected Multiple, got " + result);
        }
    }

    @Test
    void packageResolvesClaimContentViaStore() {
        var store = new MemoryContentStore();
        byte[] payload = "stored".getBytes(StandardCharsets.UTF_8);
        String id = store.store(payload);
        var ff = FlowFile.Companion.create(new ClaimContent(id, payload.length), Map.of("k", "v"));

        if (!(new PackageFlowFileV3(store).process(ff) instanceof ProcessorResult.Single single)) {
            fail("expected Single from pack");
            return;
        }
        byte[] packedBytes = switch (single.flowFile.getContent()) {
            case RawContent raw -> raw.getBytes();
            default -> { fail("pack output should be raw"); yield new byte[0]; }
        };

        var restored = FlowFileV3.unpack(packedBytes, 0).flowFile;
        assert restored != null;
        assertEquals("v", restored.getAttributes().get("k"));
        if (restored.getContent() instanceof RawContent rc) {
            assertEquals("stored", new String(rc.getBytes(), StandardCharsets.UTF_8));
        } else {
            fail("expected RawContent, got " + restored.getContent().getClass().getSimpleName());
        }
    }
}
