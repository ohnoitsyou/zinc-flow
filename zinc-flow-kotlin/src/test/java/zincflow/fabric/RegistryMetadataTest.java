package zincflow.fabric;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Objects;

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
            assert info != null;
            assertNotEquals("Other", info.getCategory(),
                    info.getName() + ": built-ins must declare a real category");
            assertEquals(info.getParameters().size(), info.getConfigKeys().size(),
                    info.getName() + ": configKeys mirrors parameters");
            var seen = new HashSet<String>();
            for (ParamInfo p : info.getParameters()) {
                assertNotNull(p.name);
                assertFalse(p.name.isEmpty(), info.getName() + ": param name non-empty");
                assertTrue(seen.add(p.name),
                        info.getName() + ": duplicate param '" + p.name + "'");
            }
        }
    }

    @Test
    void enumParametersHaveChoices() {
        for (Registry.TypeInfo info : new Registry().listAll()) {
            assert info != null;
            for (ParamInfo p : info.getParameters()) {
                if (p.getKind() == ParamKind.ENUM) {
                    assertNotNull(p.getChoices(), info.getName() + "." + p.name + ": ENUM needs choices");
                    assertFalse(Objects.requireNonNull(p.getChoices()).isEmpty(),
                            info.getName() + "." + p.name + ": ENUM choices non-empty");
                    if (p.defaultValue != null) {
                        assertTrue(p.getChoices().contains(p.defaultValue),
                                info.getName() + "." + p.name + ": default '" + p.defaultValue
                                        + "' is in choices");
                    }
                }
            }
        }
    }

    @Test
    void keyValueListCarriesDelimiters() {
        for (Registry.TypeInfo info : new Registry().listAll()) {
            assert info != null;
            for (ParamInfo p : info.getParameters()) {
                if (p.getKind() == ParamKind.KEY_VALUE_LIST) {
                    assertNotNull(p.getEntryDelim());
                    assertFalse(p.getEntryDelim().isEmpty(),
                            info.getName() + "." + p.name + ": entry delim non-empty");
                    assertNotNull(p.getPairDelim());
                    assertFalse(p.getPairDelim().isEmpty(),
                            info.getName() + "." + p.name + ": pair delim non-empty");
                }
            }
        }
    }

    @Test
    void legacyConstructorStillWorks() {
        var info = new Registry.TypeInfo("X", "1.0.0", "test",
                java.util.List.of("a", "b"), java.util.List.of("success"));
        assertEquals("Other", info.getCategory());
        assertEquals(2, info.getParameters().size());
        assertEquals("a", info.getParameters().getFirst().name);
        assertEquals(ParamKind.STRING, info.getParameters().getFirst().getKind());
        assertEquals("a", info.getParameters().getFirst().getLabel());
    }

    @Test
    void routeRecordShape() {
        var r = new Registry();
        var info = r.latest("RouteRecord");
        assertNotNull(info, "RouteRecord registered");
        assertEquals("Routing", info.getCategory());
        assertEquals(1, info.getParameters().size());
        var routes = info.getParameters().getFirst();
        assertEquals("routes", routes.name);
        assertEquals(ParamKind.KEY_VALUE_LIST, routes.getKind());
        assertTrue(routes.required);
        assertEquals(ParamKind.EXPRESSION, routes.valueKind);
        assertEquals(";", routes.getEntryDelim());
        assertEquals(":", routes.getPairDelim());
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
