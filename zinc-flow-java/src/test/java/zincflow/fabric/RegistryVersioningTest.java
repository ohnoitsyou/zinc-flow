package zincflow.fabric;

import org.junit.jupiter.api.Test;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

final class RegistryVersioningTest {

    private static Processor noop() {
        return ff -> new ProcessorResult.Dropped();
    }

    @Test
    void defaultRegisterAssignsDefaultVersion() {
        var r = new Registry();
        r.register("Toy", (cfg, ctx) -> noop());
        assertTrue(r.has("Toy"));
        assertTrue(r.has("Toy@" + TypeRefs.DEFAULT_VERSION));
        assertEquals(TypeRefs.DEFAULT_VERSION, Objects.requireNonNull(r.latest("Toy")).getVersion());
    }

    @Test
    void multipleVersionsCoexistAndLatestWins() {
        var r = new Registry();
        r.register("Toy", "1.0.0", (cfg, ctx) -> noop());
        r.register("Toy", "1.2.0", (cfg, ctx) -> noop());
        r.register("Toy", "1.1.5", (cfg, ctx) -> noop());
        assertEquals("1.2.0", Objects.requireNonNull(r.latest("Toy")).getVersion());

        // Unqualified create → latest
        assertNotNull(r.create("Toy", Map.of()));
        // Pinned create → specific version
        assertNotNull(r.create("Toy@1.0.0", Map.of()));
        assertNotNull(r.create("Toy@1.1.5", Map.of()));
    }

    @Test
    void unknownTypeAndVersionRejected() {
        var r = new Registry();
        r.register("Toy", "1.0.0", (cfg, ctx) -> noop());
        assertThrows(IllegalArgumentException.class, () -> r.create("Missing", Map.of()));
        assertThrows(IllegalArgumentException.class, () -> r.create("Toy@9.9.9", Map.of()));
    }

    @Test
    void listAllSortsByNameThenAscendingVersion() {
        var r = new Registry();
        r.register("Beta", "2.0.0", (cfg, ctx) -> noop());
        r.register("Alpha", "1.5.0", (cfg, ctx) -> noop());
        r.register("Alpha", "1.0.0", (cfg, ctx) -> noop());

        List<Registry.TypeInfo> all = r.listAll();
        // Built-ins are present too — filter.
        List<Registry.TypeInfo> ours = all.stream()
                .filter(t -> t.getName().equals("Alpha") || t.getName().equals("Beta"))
                .toList();
        assertEquals("Alpha", ours.get(0).getName());
        assertEquals("1.0.0", ours.get(0).getVersion());
        assertEquals("Alpha", ours.get(1).getName());
        assertEquals("1.5.0", ours.get(1).getVersion());
        assertEquals("Beta", ours.get(2).getName());
    }

    @Test
    void listVersionsPerType() {
        var r = new Registry();
        r.register("Toy", "1.0.0", (cfg, ctx) -> noop());
        r.register("Toy", "2.0.0", (cfg, ctx) -> noop());
        List<Registry.TypeInfo> versions = r.listVersions("Toy");
        assertEquals(2, versions.size());
        assertEquals("1.0.0", versions.get(0).getVersion());
        assertEquals("2.0.0", versions.get(1).getVersion());
        assertTrue(r.listVersions("Ghost").isEmpty());
    }

    @Test
    void typeRefParses() {
        assertEquals(new TypeRefs.TypeRef("Foo", null), TypeRefs.TypeRef.Companion.parse("Foo"));
        assertEquals(new TypeRefs.TypeRef("Foo", "1.2.3"), TypeRefs.TypeRef.Companion.parse("Foo@1.2.3"));
        assertEquals("Foo@1.2.3", new TypeRefs.TypeRef("Foo", "1.2.3").raw());
        assertEquals("Foo", new TypeRefs.TypeRef("Foo", null).raw());
    }

    @Test
    void versionedConfigTypeParsesThroughConfigLoader() {
        var r = new Registry();
        r.register("Toy", "1.0.0", (cfg, ctx) -> noop());
        r.register("Toy", "2.0.0", (cfg, ctx) -> noop());
        var loader = new ConfigLoader(r);
        var graph = loader.load("""
                flow:
                  entryPoints: [ingress]
                  processors:
                    ingress:
                      type: Toy@1.0.0
                """);
        assertEquals(1, graph.getProcessors().size());
        assertTrue(graph.getProcessors().containsKey("ingress"));
    }

    @Test
    void configWithUnknownVersionFailsCleanly() {
        var r = new Registry();
        r.register("Toy", "1.0.0", (cfg, ctx) -> noop());
        var loader = new ConfigLoader(r);
        assertThrows(IllegalArgumentException.class, () -> loader.load("""
                flow:
                  entryPoints: [ingress]
                  processors:
                    ingress:
                      type: Toy@9.9.9
                """));
    }

    @Test
    void compareVersionsHandlesVariableSegmentCount() {
        assertEquals(0, TypeRefs.INSTANCE.compareVersions("1.2", "1.2.0"));
        assertTrue(TypeRefs.INSTANCE.compareVersions("1.2.0", "1.2.1") < 0);
        assertTrue(TypeRefs.INSTANCE.compareVersions("2.0.0", "1.9.9") > 0);
    }
}
