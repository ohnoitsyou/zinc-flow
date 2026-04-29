package zincflow.providers;

import org.junit.jupiter.api.Test;
import zincflow.core.ComponentState;

import static org.junit.jupiter.api.Assertions.*;

final class SchemaRegistryProviderTest {

    @Test
    void registerBumpsVersionPerSubject() {
        var reg = new SchemaRegistryProvider();
        var v1 = reg.register("order", "{\"type\":\"record\",\"v\":1}");
        var v2 = reg.register("order", "{\"type\":\"record\",\"v\":2}");
        assertEquals(1, v1.version);
        assertEquals(2, v2.version);
        assertEquals(2, reg.latest("order").orElseThrow().version);
    }

    @Test
    void sameDefinitionGetsStableIdAcrossSubjects() {
        // Core Confluent invariant: one global id per unique schema,
        // regardless of how many subjects it's registered under.
        var reg = new SchemaRegistryProvider();
        var a = reg.register("order-value", "{\"type\":\"string\"}");
        var b = reg.register("user-value", "{\"type\":\"string\"}");
        assertEquals(a.id, b.id, "same definition under different subjects must share id");
    }

    @Test
    void idempotentRegisterReturnsExistingVersion() {
        var reg = new SchemaRegistryProvider();
        var first = reg.register("order", "X");
        var second = reg.register("order", "X");
        assertEquals(first.version, second.version,
                "re-registering an already-known definition must not bump version");
        assertEquals(1, reg.listVersions("order").size());
    }

    @Test
    void differentSubjectsAreIndependent() {
        var reg = new SchemaRegistryProvider();
        reg.register("order", "{}");
        reg.register("user", "{}");
        reg.register("order", "{\"v\":2}");
        assertEquals(2, reg.latest("order").orElseThrow().version);
        assertEquals(1, reg.latest("user").orElseThrow().version);
        assertEquals(2, reg.listSubjects().size());
    }

    @Test
    void getByIdResolvesAcrossSubjects() {
        var reg = new SchemaRegistryProvider();
        var s = reg.register("order", "{\"type\":\"record\"}");
        assertEquals(s.definition, reg.getById(s.id).orElseThrow().definition);
        assertTrue(reg.getById(999).isEmpty());
    }

    @Test
    void getEntryFetchesSpecificVersion() {
        var reg = new SchemaRegistryProvider();
        reg.register("order", "v1");
        reg.register("order", "v2");
        assertEquals("v1", reg.getEntry("order", 1).orElseThrow().definition);
        assertEquals("v2", reg.getEntry("order", 2).orElseThrow().definition);
        assertTrue(reg.getEntry("order", 99).isEmpty());
        assertTrue(reg.getEntry("unknown", 1).isEmpty());
    }

    @Test
    void listVersionsReturnsVersionNumbersInOrder() {
        var reg = new SchemaRegistryProvider();
        reg.register("order", "v1");
        reg.register("order", "v2");
        reg.register("order", "v3");
        assertEquals(java.util.List.of(1, 2, 3), reg.listVersions("order"));
    }

    @Test
    void deleteSubjectReturnsRemovedVersions() {
        var reg = new SchemaRegistryProvider();
        reg.register("order", "v1");
        reg.register("order", "v2");
        assertEquals(java.util.List.of(1, 2), reg.deleteSubject("order"));
        assertTrue(reg.listSubjects().isEmpty());
        assertTrue(reg.deleteSubject("order").isEmpty(),
                "second delete on same subject is an empty no-op, not an error");
    }

    @Test
    void deleteVersionKeepsOtherVersions() {
        var reg = new SchemaRegistryProvider();
        reg.register("order", "v1");
        reg.register("order", "v2");
        reg.register("order", "v3");
        assertTrue(reg.deleteVersion("order", 2));
        assertEquals(java.util.List.of(1, 3), reg.listVersions("order"),
                "deleting v2 must not renumber v3 back to v2");
        assertFalse(reg.deleteVersion("order", 99));
        assertFalse(reg.deleteVersion("ghost", 1));
    }

    @Test
    void lifecycleTogglesStateAndClearsOnShutdown() {
        var reg = new SchemaRegistryProvider();
        assertEquals(ComponentState.DISABLED, reg.state());
        reg.enable();
        assertEquals(ComponentState.ENABLED, reg.state());

        reg.register("order", "{}");
        assertEquals(1, reg.size());

        reg.disable(0);
        assertEquals(ComponentState.DISABLED, reg.state());
        assertEquals(1, reg.size(), "disable keeps schemas");

        reg.shutdown();
        assertTrue(reg.listSubjects().isEmpty(), "shutdown drops every registered schema");
    }

    @Test
    void rejectsBlankArguments() {
        assertThrows(IllegalArgumentException.class, () ->
                new SchemaRegistryProvider.Schema(1, "", 1, "{}"));
        assertThrows(IllegalArgumentException.class, () ->
                new SchemaRegistryProvider.Schema(1, "s", 0, "{}"));
        assertThrows(IllegalArgumentException.class, () ->
                new SchemaRegistryProvider.Schema(0, "s", 1, "{}"));
        assertThrows(IllegalArgumentException.class, () ->
                new SchemaRegistryProvider.Schema(1, "s", 1, null));
    }
}
