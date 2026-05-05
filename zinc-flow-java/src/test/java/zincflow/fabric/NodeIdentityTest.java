package zincflow.fabric;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class NodeIdentityTest {

    @Test
    void configOverrideWinsOverPersistedFile(@TempDir Path dir) {
        Path nodeIdFile = dir.resolve("zincflow.nodeId");
        var identity = NodeIdentity.Companion.resolve(
                Map.of("ui", Map.of("nodeId", "pinned-id")),
                nodeIdFile,
                "9.9.9");
        assertEquals("pinned-id", identity.getNodeId());
        assertFalse(Files.exists(nodeIdFile),
                "config override should not cause the file to be written");
    }

    @Test
    void uuidPersistedOnFirstBoot(@TempDir Path dir) throws Exception {
        Path nodeIdFile = dir.resolve("zincflow.nodeId");
        var identity = NodeIdentity.Companion.resolve(Map.of(), nodeIdFile, "1.0.0");
        assertTrue(Files.exists(nodeIdFile));
        assertEquals(identity.getNodeId(), Files.readString(nodeIdFile).trim());
    }

    @Test
    void persistedUuidSurvivesSecondResolve(@TempDir Path dir) {
        Path nodeIdFile = dir.resolve("zincflow.nodeId");
        var first = NodeIdentity.Companion.resolve(Map.of(), nodeIdFile, "1.0.0");
        var second = NodeIdentity.Companion.resolve(Map.of(), nodeIdFile, "1.0.0");
        assertEquals(first.getNodeId(), second.getNodeId());
    }

    @Test
    void toMapIncludesExpectedKeys(@TempDir Path dir) {
        var identity = NodeIdentity.Companion.resolve(
                Map.of("ui", Map.of("nodeId", "n1")), dir.resolve("x"), "2.0.0");
        Map<String, Object> body = identity.toMap(9092);
        assertEquals("n1", body.get("nodeId"));
        assertEquals(9092, body.get("port"));
        assertEquals("2.0.0", body.get("version"));
        assertNotNull(body.get("hostname"));
        assertNotNull(body.get("uptimeMillis"));
    }
}
