package zincflow.fabric;

import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

/// Mirrors zinc-flow-csharp's RegistryMetadataTests — every built-in
/// exposes a coherent typed ParamInfo list so the UI's schema-driven form
/// works on both tracks.
final class RegistryMetadataTest {

    @Test
    void everyBuiltinHasCategoryAndParameters() {
        var r = new Registry();
        assertFalse(r.listAll().isEmpty(), "registry is non-empty");
        for (Registry.TypeInfo info : r.listAll()) {
            assertNotEquals("Other", info.category(),
                    info.name + ": built-ins must declare a real category");
            assertEquals(info.parameters().size(), info.configKeys().size(),
                    info.name + ": configKeys mirrors parameters");
            var seen = new HashSet<String>();
            for (ParamInfo p : info.parameters()) {
                assertNotNull(p.name);
                assertFalse(p.name.isEmpty(), info.name + ": param name non-empty");
                assertTrue(seen.add(p.name),
                        info.name + ": duplicate param '" + p.name + "'");
            }
        }
    }

    @Test
    void enumParametersHaveChoices() {
        for (Registry.TypeInfo info : new Registry().listAll()) {
            for (ParamInfo p : info.parameters()) {
                if (p.kind() == ParamKind.ENUM) {
                    assertNotNull(p.choices(), info.name + "." + p.name + ": ENUM needs choices");
                    assertFalse(p.choices().isEmpty(),
                            info.name + "." + p.name + ": ENUM choices non-empty");
                    if (p.defaultValue != null) {
                        assertTrue(p.choices().contains(p.defaultValue),
                                info.name + "." + p.name + ": default '" + p.defaultValue
                                        + "' is in choices");
                    }
                }
            }
        }
    }

    @Test
    void keyValueListCarriesDelimiters() {
        for (Registry.TypeInfo info : new Registry().listAll()) {
            for (ParamInfo p : info.parameters()) {
                if (p.kind() == ParamKind.KEY_VALUE_LIST) {
                    assertNotNull(p.entryDelim());
                    assertFalse(p.entryDelim().isEmpty(),
                            info.name + "." + p.name + ": entry delim non-empty");
                    assertNotNull(p.pairDelim());
                    assertFalse(p.pairDelim().isEmpty(),
                            info.name + "." + p.name + ": pair delim non-empty");
                }
            }
        }
    }

    @Test
    void legacyConstructorStillWorks() {
        var info = new Registry.TypeInfo("X", "1.0.0", "test",
                java.util.List.of("a", "b"), java.util.List.of("success"));
        assertEquals("Other", info.category());
        assertEquals(2, info.parameters().size());
        assertEquals("a", info.parameters().get(0).name);
        assertEquals(ParamKind.STRING, info.parameters().get(0).kind());
        assertEquals("a", info.parameters().get(0).label());
    }

    @Test
    void routeRecordShape() {
        var r = new Registry();
        var info = r.latest("RouteRecord");
        assertNotNull(info, "RouteRecord registered");
        assertEquals("Routing", info.category());
        assertEquals(1, info.parameters().size());
        var routes = info.parameters().get(0);
        assertEquals("routes", routes.name);
        assertEquals(ParamKind.KEY_VALUE_LIST, routes.kind());
        assertTrue(routes.required);
        assertEquals(ParamKind.EXPRESSION, routes.valueKind);
        assertEquals(";", routes.entryDelim());
        assertEquals(":", routes.pairDelim());
        assertNotNull(routes.placeholder);
    }

    @Test
    void newPrimitivesRegistered() {
        var r = new Registry();
        assertTrue(r.has("RouteRecord"));
        assertTrue(r.has("UpdateRecord"));
        assertTrue(r.has("SplitRecord"));
    }

    @Test
    void paramKindJsonNameMatchesCSharp() {
        // Shared React UI treats kind strings case-sensitively. Java's
        // SCREAMING_SNAKE_CASE enum names must serialize as PascalCase to
        // match the C# worker.
        assertEquals("String", ParamKind.STRING.jsonName());
        assertEquals("KeyValueList", ParamKind.KEY_VALUE_LIST.jsonName());
        assertEquals("StringList", ParamKind.STRING_LIST.jsonName());
        assertEquals("Expression", ParamKind.EXPRESSION.jsonName());
    }
}
