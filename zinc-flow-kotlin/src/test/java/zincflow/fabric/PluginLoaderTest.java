package zincflow.fabric;

import org.jetbrains.annotations.NotNull;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import zincflow.core.ComponentState;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorContext;
import zincflow.core.ProcessorPlugin;
import zincflow.core.ProcessorResult;
import zincflow.core.Provider;
import zincflow.core.ProviderPlugin;

import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.*;

final class PluginLoaderTest {

    /// Classpath-level discovery — the test resources under
    /// {@code src/test/resources/META-INF/services/} advertise
    /// {@link FixtureProcessorPlugin} and {@link FixtureProviderPlugin}.
    @Test
    void discoversPluginsOnTestClasspath() {
        var registry = new ProcessorRegistry();
        var ctx = new ProcessorContext();
        var summary = PluginLoader.load(getClass().getClassLoader(), registry, ctx);

        assertTrue(summary.getProcessorTypes().contains("FixtureProcessor@1.0.0"),
                "expected FixtureProcessor@1.0.0 in " + summary.getProcessorTypes());
        assertTrue(summary.getProviderNames().contains("fixture"),
                "expected fixture provider in " + summary.getProviderNames());
        assertNotNull(registry.create("FixtureProcessor", Map.of(), ctx));
        assertNotNull(ctx.getProvider("fixture"));
    }

    /// End-to-end JAR-drop: build a throwaway jar with service entries
    /// pointing at classes that already live on the test classpath
    /// (ServiceLoader doesn't require the classes to physically live in
    /// the jar — as long as the classloader can resolve them from the
    /// parent). This exercises the directory-scan path without bringing
    /// in a compile-classes-at-test-time dependency.
    @Test
    void discoversPluginsFromDroppedJar(@TempDir Path tempDir) throws Exception {
        Path pluginsDir = Files.createDirectory(tempDir.resolve("plugins"));
        Path jarPath = pluginsDir.resolve("drop-plugin.jar");
        writeServiceJar(jarPath,
                Map.of(
                        "META-INF/services/zincflow.core.ProcessorPlugin",
                        "zincflow.fabric.PluginLoaderTest$DropInProcessorPlugin\n",
                        "META-INF/services/zincflow.core.ProviderPlugin",
                        "zincflow.fabric.PluginLoaderTest$DropInProviderPlugin\n"));

        var registry = new ProcessorRegistry();
        var ctx = new ProcessorContext();
        var summary = PluginLoader.loadFromDirectory(pluginsDir, registry, ctx);

        assertEquals(1, Objects.requireNonNull(summary.getJars()).size());
        assertEquals(jarPath, summary.getJars().getFirst());
        assertTrue(summary.getProcessorTypes().contains("DropIn@1.0.0"));
        assertTrue(summary.getProviderNames().contains("dropin"));
        assertNotNull(ctx.getProvider("dropin"));

        // Processor registered in Registry is live.
        Processor p = registry.create("DropIn", Map.of(), ctx);
        assertInstanceOf(DropInProcessor.class, p);
    }

    @Test
    void missingDirectoryYieldsEmptySummary(@TempDir Path tempDir) {
        Path missing = tempDir.resolve("nope");
        var registry = new ProcessorRegistry();
        var ctx = new ProcessorContext();
        var summary = PluginLoader.loadFromDirectory(missing, registry, ctx);
        assertEquals(0, summary.totalLoaded());
        assertEquals(0, Objects.requireNonNull(summary.getJars()).size());
    }

    @Test
    void emptyDirectoryYieldsEmptySummary(@TempDir Path tempDir) throws Exception {
        Path empty = Files.createDirectory(tempDir.resolve("empty"));
        var summary = PluginLoader.loadFromDirectory(empty, new ProcessorRegistry(), new ProcessorContext());
        assertEquals(0, summary.totalLoaded());
    }

    @Test
    void nullClassLoaderReturnsNoPlugins() {
        var registry = new ProcessorRegistry();
        var ctx = new ProcessorContext();
        // ClassLoader.getSystemClassLoader() won't have the test resources
        // (they live on the test classpath). Use it to prove the scan is
        // actually classloader-scoped.
        var emptyLoader = new URLClassLoader(new java.net.URL[0], null);
        var summary = PluginLoader.load(emptyLoader, registry, ctx);
        assertFalse(summary.getProcessorTypes().contains("FixtureProcessor@1.0.0"));
        assertFalse(summary.getProviderNames().contains("fixture"));
    }

    // --- Fixture service implementations ---

    public static final class FixtureProcessor implements Processor {
        @Override public ProcessorResult process(FlowFile ff) { return ProcessorResult.Single.Companion.invoke(ff); }
    }

    public static final class FixtureProcessorPlugin implements ProcessorPlugin {
        @Override public String type() { return "FixtureProcessor"; }
        @Override public Processor create(Map<String, String> config, ProcessorContext context) {
            return new FixtureProcessor();
        }
    }

    public static final class FixtureProvider implements Provider {
        private ComponentState state = ComponentState.ENABLED;
        @Override public String name() { return "fixture"; }
        @Override public String providerType() { return "fixture"; }
        @Override public ComponentState state() { return state; }
        @Override public void enable() { state = ComponentState.ENABLED; }
        @Override public void disable(int drainTimeoutSeconds) { state = ComponentState.DISABLED; }
        @Override public void shutdown() { state = ComponentState.DISABLED; }
    }

    public static final class FixtureProviderPlugin implements ProviderPlugin {
        @Override public String providerType() { return "fixture"; }

        @Override
        public Provider create(@NotNull Map<String, ?> config) { return new FixtureProvider(); }
    }

    public static final class DropInProcessor implements Processor {
        @Override public ProcessorResult process(FlowFile ff) { return new ProcessorResult.Dropped(); }
    }

    public static final class DropInProcessorPlugin implements ProcessorPlugin {
        @Override public String type() { return "DropIn"; }
        @Override public Processor create(Map<String, String> config, ProcessorContext context) {
            return new DropInProcessor();
        }
    }

    public static final class DropInProvider implements Provider {
        private ComponentState state = ComponentState.ENABLED;
        @Override public String name() { return "dropin"; }
        @Override public String providerType() { return "dropin"; }
        @Override public ComponentState state() { return state; }
        @Override public void enable() { state = ComponentState.ENABLED; }
        @Override public void disable(int drainTimeoutSeconds) { state = ComponentState.DISABLED; }
        @Override public void shutdown() { state = ComponentState.DISABLED; }
    }

    public static final class DropInProviderPlugin implements ProviderPlugin {
        @Override public @NonNull String providerType() { return "dropin"; }
        @Override public Provider create(@NotNull Map<String, ?> config) { return new DropInProvider(); }
    }

    /// Build a minimal jar at {@code jarPath} containing only the given
    /// files (typically META-INF/services entries). The fixture classes
    /// themselves live on the test classpath and are resolved through
    /// the URLClassLoader's parent chain — which is exactly the
    /// "drop-in plugin that depends on zincflow-core" shape that
    /// production plugins will have.
    private static void writeServiceJar(Path jarPath, Map<String, String> entries) throws Exception {
        try (var fileOut = Files.newOutputStream(jarPath);
             var jar = new JarOutputStream(fileOut)) {
            for (var entry : entries.entrySet()) {
                jar.putNextEntry(new JarEntry(entry.getKey()));
                jar.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
                jar.closeEntry();
            }
        }
    }
}
