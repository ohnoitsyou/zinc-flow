package zincflow.fabric;

import org.junit.jupiter.api.Test;
import zincflow.core.Source;
import zincflow.sources.GenerateFlowFile;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the new {@code sources:} YAML shape dispatches through
/// {@link SourceRegistry} — the ConfigLoader is no longer the place
/// that decides which source types exist.
final class SourceConfigTest {

    @Test
    void parsesTypedSourcesFromConfig() {
        var registry = new Registry();
        var sources = new SourceRegistry();
        PluginLoader.loadSources(getClass().getClassLoader(), sources);
        assertTrue(sources.has("GenerateFlowFile"), "built-in source must register via ServiceLoader");

        var loader = new ConfigLoader(registry, null, sources);
        loader.load("""
                flow:
                  entryPoints: [noop]
                  processors:
                    noop:
                      type: LogAttribute
                sources:
                  heartbeat:
                    type: GenerateFlowFile
                    config:
                      content: "ping"
                      batchSize: 3
                """);

        List<Source> built = loader.lastSources();
        assertEquals(1, built.size());
        assertEquals("heartbeat", built.getFirst().name());
        assertEquals(GenerateFlowFile.TYPE, built.getFirst().sourceType());
    }

    @Test
    void unknownTypeThrowsNotIgnored() {
        var registry = new Registry();
        var sources = new SourceRegistry();
        PluginLoader.loadSources(getClass().getClassLoader(), sources);

        var loader = new ConfigLoader(registry, null, sources);
        var ex = assertThrows(IllegalArgumentException.class, () -> loader.load("""
                flow:
                  entryPoints: [noop]
                  processors:
                    noop:
                      type: LogAttribute
                sources:
                  bad:
                    type: DoesNotExist
                """));
        assertTrue(ex.getMessage().contains("DoesNotExist"));
    }

    @Test
    void factoryReturningNullIsTreatedAsDisabled() {
        var registry = new Registry();
        var sources = new SourceRegistry();
        PluginLoader.loadSources(getClass().getClassLoader(), sources);

        var loader = new ConfigLoader(registry, null, sources);
        // GetFile without inputDir → Plugin.create returns null.
        loader.load("""
                flow:
                  entryPoints: [noop]
                  processors:
                    noop:
                      type: LogAttribute
                sources:
                  infile:
                    type: GetFile
                    config: {}
                """);
        assertTrue(loader.lastSources().isEmpty());
    }
}
