package zincflow.fabric

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import zincflow.core.ProcessorContext
import zincflow.core.ProcessorPlugin
import zincflow.core.ProviderPlugin
import zincflow.core.SourcePlugin
import java.io.IOException
import java.net.MalformedURLException
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.util.ServiceLoader
import kotlin.io.path.extension

/** Discovers [ProcessorPlugin] and [ProviderPlugin]
 * services on a given [ClassLoader] and wires them into a
 * [ProcessorRegistry] + [ProcessorContext]. Providers load first
 * so processor plugins built on top of them find the dependency in
 * the context.
 * 
 * <h2>Shape</h2>
 * Two static entry points:
 * 
 *  * [.load] — scan
 * a specific classloader. Used in tests + for the JVM's system
 * classloader after an ad-hoc add.
 *  * [.loadFromDirectory] —
 * build a [URLClassLoader] over every `*.jar` in
 * the directory, then scan it. Used in production via the
 * `$ZINCFLOW_PLUGINS_DIR` (default `./plugins`) hook
 * in [zincflow.Zinc].
 * 
 * 
 * Both return a [Summary] listing the registered names so the
 * management API and startup logs can report what came in. */
object PluginLoader {
    private val log: Logger = LoggerFactory.getLogger(PluginLoader::class.java)

    /** Discover plugins on the given classloader and register them.
     * Providers first (processors may require them at create-time),
     * then processors, then sources (sources are independent but their
     * factories may look up providers via the context at start-time). */
    @JvmStatic
    @JvmOverloads
    fun load(
        cl: ClassLoader, processorRegistry: ProcessorRegistry, context: ProcessorContext,
        sourceRegistry: SourceRegistry? = null
    ): Summary {
        val providers = loadProvidersLegacy(cl, context)
        val processors = loadProcessors(cl, processorRegistry)
        val sources = if (sourceRegistry == null) listOf() else loadSources(cl, sourceRegistry)
        return Summary(processors, providers, sources, null, listOf(), null)
    }

    /** Scan `dir` for `*.jar` files, stitch them into a
     * [URLClassLoader] layered on top of the current classloader,
     * and register every plugin service they expose. Missing or empty
     * directories are not an error — they yield an empty summary. */
    @JvmStatic
    @JvmOverloads
    fun loadFromDirectory(
        dir: Path,
        processorRegistry: ProcessorRegistry,
        context: ProcessorContext,
        sourceRegistry: SourceRegistry? = null
    ): Summary {
        if (!Files.isDirectory(dir)) {
            log.warn("Path '$dir' is not a directory")
            return Summary.empty()
        }
        val jars = try {
            Files.list(dir).use { entries ->
                entries
                    .filter { it.fileName.extension == "jar" }
                    .sorted().toList()
            }
        } catch (ex: IOException) {
            log.warn("Plugin directory scan failed: {} — {}", dir, ex.toString())
            return Summary(listOf(), listOf(), listOf(), dir, listOf(), null)
        }
        if (jars.isEmpty()) {
            return Summary(listOf(), listOf(), listOf(), dir, listOf(), null)
        }
        val urls = jars.mapNotNull { jar ->
            try {
                jar.toUri().toURL()
            } catch (ex: MalformedURLException) {
                // toUri().toURL() on an absolute path doesn't realistically fail,
                // but if it does the plugin is unusable — skip with a warning.
                log.warn("plugin jar has unusable URL: $jar — $ex")
                null
            }
        }
        val cl = URLClassLoader(urls.toTypedArray(), PluginLoader::class.java.classLoader)
        val providers = loadProvidersLegacy(cl, context)
        val processors = loadProcessors(cl, processorRegistry)
        val sources = if (sourceRegistry == null) listOf() else loadSources(cl, sourceRegistry)
        log.info(
            "loaded {} plugin(s) from {} — providers: {}, processors: {}, sources: {}",
            providers.size + processors.size + sources.size, dir, providers, processors, sources
        )
        return Summary(processors, providers, sources, dir, jars, cl)
    }

    /** Scan `cl` for [ProviderPlugin] services and register
     * their factories with `registry`. Actual provider
     * instantiation is deferred to config loading — the registry maps
     * `type: LoggingProvider@1.0.0` to a factory the same way
     * processors and sources do. */
    @JvmStatic
    fun loadProviders(cl: ClassLoader, registry: ProviderRegistry): List<String> {
        val types = mutableListOf<String>()
        for (plugin in ServiceLoader.load(ProviderPlugin::class.java, cl)) {
            val type = plugin.providerType()
            if (type.isEmpty()) {
                log.warn("ProviderPlugin {} reported blank providerType() — skipping", plugin.javaClass.name)
                continue
            }
            val version = plugin.version().ifEmpty { TypeRefs.DEFAULT_VERSION }
            val info = ProviderRegistry.TypeInfo(type, version, plugin.description(), plugin.configKeys())
            registry.register(info) { config: Map<String, Any> -> plugin.create(config) }
            types.add("$type@$version")
            log.info("plugin provider registered: $type@$version (${plugin.javaClass.name})")
        }
        return types.sorted()
    }

    /** Legacy path — drop a plugin directly into a [ProcessorContext]
     * without going through a registry. Used when config.yaml has no
     * `providers:` block and the caller wants everything pulled
     * in with default config. */
    private fun loadProvidersLegacy(cl: ClassLoader, context: ProcessorContext): List<String> {
        val names = mutableListOf<String>()
        for (plugin in ServiceLoader.load(ProviderPlugin::class.java, cl)) {
            try {
                val p = plugin.create(mapOf()) ?: continue
                // Skip if the bootstrap already wired a provider under
                // this name — Main instantiates a default set before
                // scanning plugins, and we don't want a built-in
                // ServiceLoader entry (from the main jar itself) to
                // overwrite the instance that's already enabled.
                if (context.getProvider(p.name()) != null) continue
                context.addProvider(p)
                p.enable()
                names.add(p.name())
            } catch (ex: RuntimeException) {
                log.warn("ProviderPlugin ${plugin.javaClass.name} threw on create: $ex")
            }
        }
        return names.sorted()
    }

    private fun loadProcessors(cl: ClassLoader, processorRegistry: ProcessorRegistry): List<String> {
        val types = mutableListOf<String>()
        for (plugin in ServiceLoader.load(ProcessorPlugin::class.java, cl)) {
            val type = plugin.type()
            if (type.isEmpty()) {
                log.warn("ProcessorPlugin ${plugin.javaClass.name} reported blank type() — skipping")
                continue
            }
            val version = plugin.version().ifEmpty { TypeRefs.DEFAULT_VERSION }
            val info = ProcessorRegistry.TypeInfo(
                type, version,
                plugin.description(),
                plugin.configKeys(),
                plugin.relationships()
            )
            processorRegistry.register(info) { config: Map<String, String>, context: ProcessorContext ->
                plugin.create(config, context)
            }
            types.add("$type@$version")
            log.info("plugin processor registered: $type@$version (${plugin.javaClass.name})")
        }
        return types.sorted()
    }

    /** Scan `cl` for [SourcePlugin] services and register
     * them with `registry`. Exposed for the Main bootstrap so
     * the built-in sources (shipped as ServiceLoader entries in the
     * main jar) populate the registry through the same path as
     * plugin-jar sources. */
    @JvmStatic
    fun loadSources(cl: ClassLoader, registry: SourceRegistry): List<String> {
        val types = mutableListOf<String>()
        for (plugin in ServiceLoader.load(SourcePlugin::class.java, cl)) {
            val type = plugin.sourceType()
            if (type.isEmpty()) {
                log.warn("SourcePlugin '${plugin.javaClass.name} reported blank sourceType() — skipping")
                continue
            }
            val version = plugin.version().ifEmpty { TypeRefs.DEFAULT_VERSION }
            val info = SourceRegistry.TypeInfo(
                type, version,
                plugin.description(),
                plugin.configKeys()
            )
            registry.register(info) { name: String, config: Map<String, Any> ->
                plugin.create(name, config)
            }
            types.add("$type@$version")
            log.info("plugin source registered: $type@$version (${plugin.javaClass.name})")
        }
        return types.sorted()
    }

    /** Convenience — expose a summary as a JSON-friendly map for the
     * management API. Avoids Jackson having to reflect over the record. */
    fun toJson(s: Summary): Map<String, Any?> {
        return buildMap {
            put("directory", s.directory?.toString())
            put("jars", s.jars?.stream()?.map { obj: Path? -> obj.toString() })
            put("processorTypes", s.processorTypes)
            put("providerNames", s.providerNames)
            put("sourceTypes", s.sourceTypes)
            put("totalLoaded", s.totalLoaded)
        }
    }

    /** `classLoader` is the [URLClassLoader] spawned for the
     * plugin jars — retained on the summary so the owner (typically
     * [HttpServer]) can close it before replacing it on the next
     * `POST /api/plugins/reload`. Null when no classloader was
     * created (empty directory, classpath-only scan). */
    class Summary(
        val processorTypes: List<String>,
        val providerNames: List<String>,
        val sourceTypes: List<String>,
        val directory: Path?,
        val jars: List<Path>?,
        val classLoader: URLClassLoader?
    ) : AutoCloseable {
        val totalLoaded = processorTypes.size + providerNames.size + sourceTypes.size

        /** Release the [URLClassLoader] — best effort. Safe to call
         * on an [.empty] summary. */
        override fun close() {
            if (classLoader == null) return
            try {
                classLoader.close()
            } catch (ex: IOException) {
                log.warn("failed to close plugin classloader: {}", ex.toString())
            }
        }

        companion object {
            operator fun invoke(
                processorTypes: List<String>,
                providerNames: List<String>,
                sourceTypes: List<String>,
                directory: Path?,
                jars: List<Path>,
                classLoader: URLClassLoader?
            ): Summary {
                return Summary(
                    processorTypes.toList(),
                    providerNames.toList(),
                    sourceTypes.toList(),
                    directory,
                    jars.toList(),
                    classLoader
                )
            }
            fun empty(): Summary {
                return Summary(emptyList(), emptyList(), emptyList(), null, emptyList(), null)
            }
        }
    }
}
