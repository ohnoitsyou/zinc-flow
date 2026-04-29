package zincflow.fabric;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class ConfigOverlayTest {

    @Test
    void missingOverlayIsNotAnError(@TempDir Path dir) throws Exception {
        Path base = dir.resolve("config.yaml");
        Files.writeString(base, "flow:\n  entryPoints: [a]\n");
        var resolved = ConfigOverlay.load(base);
        assertTrue(resolved.layers().stream().anyMatch(l -> "base".equals(l.role) && l.present));
        assertTrue(resolved.layers().stream().anyMatch(l -> "local".equals(l.role) && !l.present));
        assertTrue(resolved.layers().stream().anyMatch(l -> "secrets".equals(l.role) && !l.present));
    }

    @Test
    void deepMergesOverlaysAndTracksProvenance(@TempDir Path dir) throws Exception {
        Path base = dir.resolve("config.yaml");
        Files.writeString(base, """
                flow:
                  processors:
                    put-http:
                      config:
                        endpoint: https://prod.example.com
                        token: FROM_BASE
                """);
        Path local = dir.resolve("config.local.yaml");
        Files.writeString(local, """
                flow:
                  processors:
                    put-http:
                      config:
                        endpoint: https://local.example.com
                """);
        Path secrets = dir.resolve("secrets.yaml");
        Files.writeString(secrets, """
                flow:
                  processors:
                    put-http:
                      config:
                        token: SECRET_TOKEN
                """);

        var resolved = ConfigOverlay.load(base);
        Map<String, Object> flow = mapUnder(resolved.effective(), "flow", "processors", "put-http", "config");
        assertEquals("https://local.example.com", flow.get("endpoint"));
        assertEquals("SECRET_TOKEN", flow.get("token"));

        // Provenance: endpoint came from local, token from secrets.
        assertEquals("local",   resolved.provenance().get("flow.processors.put-http.config.endpoint"));
        assertEquals("secrets", resolved.provenance().get("flow.processors.put-http.config.token"));
    }

    @Test
    void laterLayerOverridesEarlierLayer(@TempDir Path dir) throws Exception {
        Path base = dir.resolve("config.yaml");
        Files.writeString(base, "x: base\n");
        Path local = dir.resolve("config.local.yaml");
        Files.writeString(local, "x: local\n");
        Path secrets = dir.resolve("secrets.yaml");
        Files.writeString(secrets, "x: secret\n");

        var resolved = ConfigOverlay.load(base);
        assertEquals("secret", resolved.effective().get("x"));
        assertEquals("secrets", resolved.provenance().get("x"));
    }

    @Test
    void explicitPathsBypassEnvLookup(@TempDir Path dir) throws Exception {
        Path base = dir.resolve("config.yaml");
        Files.writeString(base, "a: base\n");
        Path local = dir.resolve("custom-local.yaml");
        Files.writeString(local, "a: local\n");

        var resolved = ConfigOverlay.load(base, local, null);
        assertEquals("local", resolved.effective().get("a"));
    }

    @Test
    void blankYamlLayerIsEmpty(@TempDir Path dir) throws Exception {
        Path base = dir.resolve("config.yaml");
        Files.writeString(base, "a: base\n");
        Path local = dir.resolve("config.local.yaml");
        Files.writeString(local, "   \n");

        var resolved = ConfigOverlay.load(base);
        assertEquals("base", resolved.effective().get("a"));
        assertEquals("base", resolved.provenance().get("a"));
    }

    @Test
    void nonMapOverlayRejected(@TempDir Path dir) throws Exception {
        Path base = dir.resolve("config.yaml");
        Files.writeString(base, "a: base\n");
        Path local = dir.resolve("config.local.yaml");
        Files.writeString(local, "- not-a-map\n");

        assertThrows(IllegalArgumentException.class, () -> ConfigOverlay.load(base));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mapUnder(Map<String, Object> root, String... keys) {
        Object current = root;
        for (String k : keys) {
            assertTrue(current instanceof Map<?, ?>,
                    "expected map at path leading to " + k + ", got " + current);
            current = ((Map<String, Object>) current).get(k);
        }
        assertTrue(current instanceof Map<?, ?>, "final node must be a map");
        return (Map<String, Object>) current;
    }
}
