package zincflow.fabric;

import kotlin.jvm.functions.Function1;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;
import zincflow.core.Source;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

final class SourceProcessorRegistryTest {

    @Test
    void registerAndCreateByBareType() {
        var r = new SourceRegistry();
        r.register("Probe", (name, cfg) -> new NoopSource(name));
        assertTrue(r.has("Probe"));
        Source s = r.create("Probe", "p1", Map.of());
        assertEquals("p1", s.name());
    }

    @Test
    void bareTypeResolvesToLatestVersion() {
        var r = new SourceRegistry();
        r.register(new SourceRegistry.TypeInfo("Probe", "1.0.0", "", List.of()),
                (n, c) -> new NoopSource(n + "@v1"));
        r.register(new SourceRegistry.TypeInfo("Probe", "2.0.0", "", List.of()),
                (n, c) -> new NoopSource(n + "@v2"));
        assertEquals("p@v2", r.create("Probe", "p", Map.of()).name());
        assertEquals("p@v1", r.create("Probe@1.0.0", "p", Map.of()).name());
    }

    @Test
    void listAllIsSortedByNameThenVersion() {
        var r = new SourceRegistry();
        r.register(new SourceRegistry.TypeInfo("B", "1.0.0", "", List.of()), (n, c) -> new NoopSource(n));
        r.register(new SourceRegistry.TypeInfo("A", "2.0.0", "", List.of()), (n, c) -> new NoopSource(n));
        r.register(new SourceRegistry.TypeInfo("A", "1.0.0", "", List.of()), (n, c) -> new NoopSource(n));
        var all = r.listAll();
        assertEquals("A", all.get(0).getName());
        assertEquals("1.0.0", all.get(0).getVersion());
        assertEquals("A", all.get(1).getName());
        assertEquals("2.0.0", all.get(1).getVersion());
        assertEquals("B", all.get(2).getName());
    }

    @Test
    void unknownTypeThrows() {
        var r = new SourceRegistry();
        var ex = assertThrows(IllegalArgumentException.class,
                () -> r.create("Ghost", "g", Map.of()));
        assertTrue(ex.getMessage().contains("Ghost"));
    }

    @Test
    void latestReturnsNullForUnknown() {
        assertNull(new SourceRegistry().latest("missing"));
    }

    private static final class NoopSource implements Source {
        private final String name;
        NoopSource(String name) { this.name = name; }
        @Override public String name() { return name; }
        @Override public String sourceType() { return "Probe"; }
        @Override public boolean isRunning() { return false; }
        @Override public void start(@NotNull Function1<? super FlowFile, Boolean> ingest) { /* no-op */ }
        @Override public void stop() { /* no-op */ }
    }
}
