package zincflow.fabric;

import org.junit.jupiter.api.Test;
import zincflow.core.Provider;
import zincflow.providers.LoggingProvider;
import zincflow.providers.ProvenanceProvider;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the {@code providers:} YAML block dispatches through
/// {@link ProviderRegistry} the same way processors and sources do.
final class ProviderConfigTest {

    @Test
    void parsesTypedProvidersFromConfig() {
        var registry = new Registry();
        var providers = new ProviderRegistry();
        PluginLoader.loadProviders(getClass().getClassLoader(), providers);
        assertTrue(providers.has(LoggingProvider.TYPE),
                "built-in LoggingProvider must register via ServiceLoader");

        var loader = new ConfigLoader(registry, null, null, providers);
        loader.load("""
                flow:
                  entryPoints: [noop]
                  processors:
                    noop:
                      type: LogAttribute
                providers:
                  logs:
                    type: LoggingProvider
                  prov:
                    type: ProvenanceProvider
                    config: {buffer: 500}
                """);

        List<Provider> built = loader.lastProviders();
        assertEquals(2, built.size());
        assertEquals(LoggingProvider.NAME, built.get(0).name());
        assertEquals(ProvenanceProvider.NAME, built.get(1).name());
    }

    @Test
    void missingProvidersBlockYieldsEmptyList() {
        var registry = new Registry();
        var providers = new ProviderRegistry();
        PluginLoader.loadProviders(getClass().getClassLoader(), providers);
        var loader = new ConfigLoader(registry, null, null, providers);
        loader.load("""
                flow:
                  entryPoints: [noop]
                  processors:
                    noop:
                      type: LogAttribute
                """);
        assertTrue(loader.lastProviders().isEmpty());
    }

    @Test
    void unknownProviderTypeThrows() {
        var registry = new Registry();
        var providers = new ProviderRegistry();
        PluginLoader.loadProviders(getClass().getClassLoader(), providers);
        var loader = new ConfigLoader(registry, null, null, providers);
        var ex = assertThrows(IllegalArgumentException.class, () -> loader.load("""
                flow:
                  entryPoints: [noop]
                  processors:
                    noop:
                      type: LogAttribute
                providers:
                  bogus:
                    type: DoesNotExist
                """));
        assertTrue(ex.getMessage().contains("DoesNotExist"));
    }
}
