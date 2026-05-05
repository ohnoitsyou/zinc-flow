package zincflow.fabric;

import org.junit.jupiter.api.Test;
import zincflow.core.Processor;
import zincflow.processors.LogAttribute;
import zincflow.processors.RouteOnAttribute;
import zincflow.processors.UpdateAttribute;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class RegistryTest {

    @Test
    void builtinsAreRegistered() {
        var r = new Registry();
        assertTrue(r.has("LogAttribute"));
        assertTrue(r.has("UpdateAttribute"));
        assertTrue(r.has("RouteOnAttribute"));
        assertTrue(r.has("FilterAttribute"));
        assertTrue(r.has("PutStdout"));
        assertTrue(r.has("PutFile"));
        assertTrue(r.has("ReplaceText"));
        assertTrue(r.has("SplitText"));
    }

    @Test
    void createReturnsTypedInstances() {
        var r = new Registry();
        assertInstanceOf(LogAttribute.class, r.create("LogAttribute", Map.of("prefix", "[x]")));
        assertInstanceOf(UpdateAttribute.class, r.create("UpdateAttribute",
                Map.of("key", "k", "value", "v")));
        assertInstanceOf(RouteOnAttribute.class, r.create("RouteOnAttribute", Map.of()));
    }

    @Test
    void missingRequiredConfigRejected() {
        var r = new Registry();
        assertThrows(IllegalArgumentException.class,
                () -> r.create("UpdateAttribute", Map.of())); // missing key
        assertThrows(IllegalArgumentException.class,
                () -> r.create("PutFile", Map.of())); // missing directory
        assertThrows(IllegalArgumentException.class,
                () -> r.create("ReplaceText", Map.of())); // missing pattern
        assertThrows(IllegalArgumentException.class,
                () -> r.create("SplitText", Map.of())); // missing delimiter
    }

    @Test
    void unknownTypeRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new Registry().create("Unknown", Map.of()));
    }

    @Test
    void customProcessorCanBeRegistered() {
        Processor trivial = ff -> new zincflow.core.ProcessorResult.Dropped();
        var r = new Registry();
        r.register("Trivial", (cfg, ctx) -> trivial);
        assertSame(trivial, r.create("Trivial", Map.of()));
    }
}
