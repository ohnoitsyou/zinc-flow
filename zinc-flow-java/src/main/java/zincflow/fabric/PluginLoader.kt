package zincflow.fabric

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import zincflow.core.ProcessorContext
import zincflow.core.ProcessorPlugin
import zincflow.core.ProviderPlugin
import zincflow.core.SourcePlugin
import java.io.IOException
import java.net.MalformedURLException
import java.net.URL
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.util.Collections
import java.util.List
import java.util.Map
import java.util.ServiceLoader
import java.util.function.Consumer
import java.util.function.Function
import java.util.function.Predicate

/** Discovers [ProcessorPlugin] and [ProviderPlugin]
 * services on a given [ClassLoader] and wires them into a
 * [Registry] + [ProcessorContext]. Providers load first
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
 * in [zincflow.Main].
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
    @JvmOverloads
    fun load(
        cl: ClassLoader?, registry: Registry, context: ProcessorContext,
        sourceRegistry: SourceRegistry? = null
    ): Summary {
        val providers = loadProvidersLegacy(cl, context)
        val processors = loadProcessors(cl, registry)
        val sources = if (sourceRegistry == null) List.of<String?>() else loadSources(cl, sourceRegistry)
        return Summary(processors, providers, sources, null, List.of<Path?>(), null)
    }

    /** Scan `dir` for `*.jar` files, stitch them into a
     * [URLClassLoader] layered on top of the current classloader,
     * and register every plugin service they expose. Missing or empty
     * directories are not an error — they yield an empty summary. */
    @JvmOverloads
    fun loadFromDirectory(
        dir: Path?, registry: Registry, context: ProcessorContext,
        sourceRegistry: SourceRegistry? = null
    ): Summary {
        if (dir == null || !Files.isDirectory(dir)) {
            return Summary(List.of<String?>(), List.of<String?>(), List.of<String?>(), dir, List.of<Path?>(), null)
        }
        val jars: MutableList<Path?> = ArrayList<Path?>()
        try {
            Files.list(dir).use { entries ->
                entries.filter(Predicate { p: Path? -> p!!.getFileName().toString().endsWith(".jar") })
                    .sorted()
                    .forEach(Consumer { e: Path? -> jars.add(e) })
            }
        } catch (ex: IOException) {
            log.warn("plugin directory scan failed: {} — {}", dir, ex.toString())
            return Summary(List.of<String?>(), List.of<String?>(), List.of<String?>(), dir, List.of<Path?>(), null)
        }
        if (jars.isEmpty()) {
            return Summary(List.of<String?>(), List.of<String?>(), List.of<String?>(), dir, List.of<Path?>(), null)
        }
        val urls = arrayOfNulls<URL>(jars.size())
        for (i in jars.indices) {
            try {
                urls[i] = jars.get(i)!!.toUri().toURL()
            } catch (ex: MalformedURLException) {
                // toUri().toURL() on an absolute path doesn't realistically fail,
                // but if it does the plugin is unusable — skip with a warning.
                log.warn("plugin jar has unusable URL: {} — {}", jars.get(i), ex.toString())
                urls[i] = null
            }
        }
        val cl = URLClassLoader(stripNulls(urls), PluginLoader::class.java.getClassLoader())
        val providers = loadProvidersLegacy(cl, context)
        val processors = loadProcessors(cl, registry)
        val sources = if (sourceRegistry == null) List.of<String?>() else loadSources(cl, sourceRegistry)
        log.info(
            "loaded {} plugin(s) from {} — providers: {}, processors: {}, sources: {}",
            providers.size() + processors.size() + sources.size(), dir, providers, processors, sources
        )
        return Summary(processors, providers, sources, dir, jars, cl)
    }

    /** Scan `cl` for [ProviderPlugin] services and register
     * their factories with `registry`. Actual provider
     * instantiation is deferred to config loading — the registry maps
     * `type: LoggingProvider@1.0.0` to a factory the same way
     * processors and sources do. */
    @JvmStatic
    fun loadProviders(cl: ClassLoader?, registry: ProviderRegistry): MutableList<String?> {
        val types: MutableList<String?> = ArrayList<String?>()
        for (plugin in ServiceLoader.load<ProviderPlugin>(ProviderPlugin::class.java, cl)) {
            val type = plugin.providerType()
            if (type == null || type.isEmpty()) {
                log.warn("ProviderPlugin {} reported blank providerType() — skipping", plugin.getClass().getName())
                continue
            }
            val version = if (plugin.version() == null || plugin.version().isEmpty())
                TypeRefs.DEFAULT_VERSION
            else
                plugin.version()
            val info = ProviderRegistry.TypeInfo(
                type, version,
                if (plugin.description() == null) "" else plugin.description(),
                if (plugin.configKeys() == null) List.of<String?>() else plugin.configKeys()
            )
            registry.register(
                info,
                ProviderRegistry.Factory { config: MutableMap<String?, Any?>? -> plugin.create(config) })
            types.add(type + "@" + version)
            log.info(
                "plugin provider registered: {}@{} ({})",
                type, version, plugin.getClass().getName()
            )
        }
        Collections.sort<String?>(types)
        return types
    }

    /** Legacy path — drop a plugin directly into a [ProcessorContext]
     * without going through a registry. Used when config.yaml has no
     * `providers:` block and the caller wants everything pulled
     * in with default config. */
    private fun loadProvidersLegacy(cl: ClassLoader?, context: ProcessorContext): MutableList<String?> {
        val names: MutableList<String?> = ArrayList<String?>()
        for (plugin in ServiceLoader.load<ProviderPlugin>(ProviderPlugin::class.java, cl)) {
            try {
                val p = plugin.create(Map.of<String?, Any?>())
                if (p == null) continue
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
                log.warn("ProviderPlugin {} threw on create: {}", plugin.getClass().getName(), ex.toString())
            }
        }
        Collections.sort<String?>(names)
        return names
    }

    private fun loadProcessors(cl: ClassLoader?, registry: Registry): MutableList<String?> {
        val types: MutableList<String?> = ArrayList<String?>()
        for (plugin in ServiceLoader.load<ProcessorPlugin>(ProcessorPlugin::class.java, cl)) {
            val type = plugin.type()
            if (type == null || type.isEmpty()) {
                log.warn("ProcessorPlugin {} reported blank type() — skipping", plugin.getClass().getName())
                continue
            }
            val version = if (plugin.version() == null || plugin.version().isEmpty())
                TypeRefs.DEFAULT_VERSION
            else
                plugin.version()
            val info = Registry.TypeInfo(
                type, version,
                if (plugin.description() == null) "" else plugin.description(),
                if (plugin.configKeys() == null) List.of<String?>() else plugin.configKeys(),
                if (plugin.relationships() == null) List.of<String?>() else plugin.relationships()
            )
            registry.register(
                info,
                Registry.Factory { config: MutableMap<String?, String>?, context: ProcessorContext? ->
                    plugin.create(
                        config,
                        context
                    )
                })
            types.add(type + "@" + version)
            log.info(
                "plugin processor registered: {}@{} ({})",
                type, version, plugin.getClass().getName()
            )
        }
        Collections.sort<String?>(types)
        return types
    }

    /** Scan `cl` for [SourcePlugin] services and register
     * them with `registry`. Exposed for the Main bootstrap so
     * the built-in sources (shipped as ServiceLoader entries in the
     * main jar) populate the registry through the same path as
     * plugin-jar sources. */
    @JvmStatic
    fun loadSources(cl: ClassLoader?, registry: SourceRegistry): MutableList<String?> {
        val types: MutableList<String?> = ArrayList<String?>()
        for (plugin in ServiceLoader.load<SourcePlugin>(SourcePlugin::class.java, cl)) {
            val type = plugin.sourceType()
            if (type == null || type.isEmpty()) {
                log.warn("SourcePlugin {} reported blank sourceType() — skipping", plugin.getClass().getName())
                continue
            }
            val version = if (plugin.version() == null || plugin.version().isEmpty())
                TypeRefs.DEFAULT_VERSION
            else
                plugin.version()
            val info = SourceRegistry.TypeInfo(
                type, version,
                if (plugin.description() == null) "" else plugin.description(),
                if (plugin.configKeys() == null) List.of<String?>() else plugin.configKeys()
            )
            registry.register(
                info,
                SourceRegistry.Factory { name: String?, config: MutableMap<String?, Any?>? ->
                    plugin.create(
                        name,
                        config
                    )
                })
            types.add(type + "@" + version)
            log.info(
                "plugin source registered: {}@{} ({})",
                type, version, plugin.getClass().getName()
            )
        }
        Collections.sort<String?>(types)
        return types
    }

    private fun stripNulls(`in`: Array<URL?>): Array<URL?> {
        var keep = 0
        for (u in `in`) if (u != null) keep++
        val out = arrayOfNulls<URL>(keep)
        var j = 0
        for (u in `in`) if (u != null) out[j++] = u
        return out
    }

    /** Convenience — expose a summary as a JSON-friendly map for the
     * management API. Avoids Jackson having to reflect over the record. */
    fun toJson(s: Summary): MutableMap<String?, Any?> {
        val out: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        out.put("directory", if (s.directory == null) null else s.directory.toString())
        out.put("jars", s.jars!!.stream().map<String?>(Function { obj: Path? -> obj.toString() }).toList())
        out.put("processorTypes", s.processorTypes)
        out.put("providerNames", s.providerNames)
        out.put("sourceTypes", s.sourceTypes)
        out.put("totalLoaded", s.totalLoaded())
        return out
    }

    /** `classLoader` is the [URLClassLoader] spawned for the
     * plugin jars — retained on the summary so the owner (typically
     * [HttpServer]) can close it before replacing it on the next
     * `POST /api/plugins/reload`. Null when no classloader was
     * created (empty directory, classpath-only scan). */
    class Summary(
        processorTypes: MutableList<String?>?,
        providerNames: MutableList<String?>?,
        sourceTypes: MutableList<String?>?,
        val directory: Path?,
        jars: MutableList<Path?>?,
        val classLoader: URLClassLoader?
    ) : AutoCloseable {
        fun totalLoaded(): Int {
            return processorTypes.size() + providerNames.size() + sourceTypes.size()
        }

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

        val processorTypes: MutableList<String?>?
        val providerNames: MutableList<String?>?
        val sourceTypes: MutableList<String?>?
        val jars: MutableList<Path?>?

        init {
            var processorTypes = processorTypes
            var providerNames = providerNames
            var sourceTypes = sourceTypes
            var jars = jars
            processorTypes = List.copyOf<String?>(processorTypes)
            providerNames = List.copyOf<String?>(providerNames)
            sourceTypes = List.copyOf<String?>(sourceTypes)
            jars = List.copyOf<Path?>(jars)
            this.processorTypes = processorTypes
            this.providerNames = providerNames
            this.sourceTypes = sourceTypes
            this.jars = jars
        }

        companion object {
            fun empty(): Summary {
                return Summary(List.of<String?>(), List.of<String?>(), List.of<String?>(), null, List.of<Path?>(), null)
            }
        }
    }
}
