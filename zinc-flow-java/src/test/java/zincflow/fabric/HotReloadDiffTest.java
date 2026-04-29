package zincflow.fabric;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/// Covers the diff path between two loads of the same ConfigLoader —
/// unchanged specs reuse processor instances, changed specs rebuild,
/// and {@link Pipeline#applyReload} accounts for each class of change.
final class HotReloadDiffTest {

    private static final String BASE_YAML = """
            flow:
              entryPoints: [a]
              processors:
                a:
                  type: LogAttribute
                  config:
                    prefix: "[a] "
                b:
                  type: UpdateAttribute
                  config:
                    key: stage
                    value: one
              connections:
                a:
                  success: [b]
            """;

    @Test
    void unchangedReloadReusesProcessorsAndReportsZeroDiff() {
        var loader = new ConfigLoader(new Registry());
        var first = loader.load(BASE_YAML);
        var pipeline = new Pipeline(first);

        var second = loader.load(BASE_YAML);
        assertSame(first.processors().get("a"), second.processors().get("a"),
                "unchanged spec should reuse processor instance");
        assertSame(first.processors().get("b"), second.processors().get("b"));

        var diff = pipeline.applyReload(second);
        assertEquals(0, diff.total());
    }

    @Test
    void updatedConfigRebuildsProcessor() {
        var loader = new ConfigLoader(new Registry());
        var first = loader.load(BASE_YAML);
        var pipeline = new Pipeline(first);

        // Change b's config value — should force b to rebuild, leave a alone.
        var mutated = BASE_YAML.replace("value: one", "value: two");
        var second = loader.load(mutated);
        assertSame(first.processors().get("a"), second.processors().get("a"));
        assertNotSame(first.processors().get("b"), second.processors().get("b"));

        var diff = pipeline.applyReload(second);
        assertEquals(0, diff.added);
        assertEquals(0, diff.removed);
        assertEquals(1, diff.updated, "b rebuilt");
    }

    @Test
    void addedAndRemovedProcessorsCount() {
        var loader = new ConfigLoader(new Registry());
        var first = loader.load(BASE_YAML);
        var pipeline = new Pipeline(first);

        String nextYaml = """
                flow:
                  entryPoints: [a]
                  processors:
                    a:
                      type: LogAttribute
                      config:
                        prefix: "[a] "
                    c:
                      type: LogAttribute
                      config:
                        prefix: "[c] "
                  connections:
                    a:
                      success: [c]
                """;
        var diff = pipeline.applyReload(loader.load(nextYaml));
        assertEquals(1, diff.added, "c is new");
        assertEquals(1, diff.removed, "b is gone");
        assertTrue(diff.connectionsChanged >= 1,
                "a's success target changed from [b] to [c]");
    }

    @Test
    void connectionChangeWithoutProcessorChangeIsItsOwnCategory() {
        var loader = new ConfigLoader(new Registry());
        var first = loader.load(BASE_YAML);
        var pipeline = new Pipeline(first);

        // Add a failure connection from a without otherwise touching anything.
        // The YAML text block strips to 4-space indent for connection keys
        // and 6-space indent for relationships.
        String nextYaml = BASE_YAML.replace(
                "    a:\n      success: [b]",
                "    a:\n      success: [b]\n      failure: [b]");
        assertNotEquals(BASE_YAML, nextYaml, "YAML mutation must actually change the text");
        var diff = pipeline.applyReload(loader.load(nextYaml));
        assertEquals(0, diff.added);
        assertEquals(0, diff.removed);
        assertEquals(0, diff.updated, "processor instances unchanged");
        assertEquals(1, diff.connectionsChanged);
    }
}
