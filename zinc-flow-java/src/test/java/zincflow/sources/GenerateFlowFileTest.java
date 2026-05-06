package zincflow.sources;

import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;
import zincflow.core.RawContent;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

final class GenerateFlowFileTest {

    @Test
    void emitsBatchOfConfiguredSizeWithExpectedAttrs() {
        GenerateFlowFile src = new GenerateFlowFile("gen", 1000,
                "hello", "text/plain", "env:dev;tenant:acme", 3);
        List<FlowFile> batch = src.poll();

        assertEquals(3, batch.size());
        for (FlowFile ff : batch) {
            assertEquals("gen", ff.getAttributes().get("source"));
            assertEquals("text/plain", ff.getAttributes().get("http.content.type"));
            assertEquals("dev", ff.getAttributes().get("env"));
            assertEquals("acme", ff.getAttributes().get("tenant"));
            assertNotNull(ff.getAttributes().get("generate.index"));
            assertInstanceOf(RawContent.class, ff.getContent());
            assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8),
                    ((RawContent) ff.getContent()).getBytes());
        }
        // indexes are distinct and strictly increasing within a batch
        long i0 = Long.parseLong(batch.get(0).getAttributes().get("generate.index"));
        long i1 = Long.parseLong(batch.get(1).getAttributes().get("generate.index"));
        long i2 = Long.parseLong(batch.get(2).getAttributes().get("generate.index"));
        assertTrue(i0 < i1 && i1 < i2);
    }

    @Test
    void emptyAttributeSpecYieldsJustTheDefaults() {
        GenerateFlowFile src = new GenerateFlowFile("gen", 1000, "x", "", "", 1);
        FlowFile ff = src.poll().getFirst();
        assertEquals("gen", ff.getAttributes().get("source"));
        assertNotNull(ff.getAttributes().get("generate.index"));
        assertNull(ff.getAttributes().get("http.content.type"));
    }

    @Test
    void malformedAttributePairsAreSkippedNotThrown() {
        GenerateFlowFile src = new GenerateFlowFile("gen", 1000, "x", "",
                "valid:ok;;no-colon;:no-key;trailing:", 1);
        FlowFile ff = src.poll().getFirst();
        assertEquals("ok", ff.getAttributes().get("valid"));
        assertEquals("", ff.getAttributes().get("trailing"));
        assertFalse(ff.getAttributes().containsKey("no-colon"));
    }

    @Test
    void nonPositiveBatchFallsBackToOne() {
        GenerateFlowFile src = new GenerateFlowFile("gen", 1000, "x", "", "", 0);
        assertEquals(1, src.poll().size());
    }
}
