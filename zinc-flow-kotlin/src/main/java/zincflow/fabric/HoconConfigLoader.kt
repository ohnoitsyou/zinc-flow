package zincflow.fabric

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigObject
import com.typesafe.config.ConfigParseOptions
import com.typesafe.config.ConfigRenderOptions
import com.typesafe.config.ConfigResolveOptions
import com.typesafe.config.ConfigSyntax
import kotlinx.serialization.EncodeDefault
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.hocon.Hocon
import kotlinx.serialization.hocon.decodeFromConfig
import kotlinx.serialization.hocon.encodeToConfig
import zincflow.fabric.HoconConfigLoader.parseOptions
import java.nio.file.Path

@Serializable
data class ZincConfig(val flow: ZincFlowConfig = ZincFlowConfig())
@Serializable
data class ZincFlowConfig(
    @EncodeDefault
    val entryPoints: List<String> = emptyList(),
    @EncodeDefault
    val processors: Map<String, ZincProcessorConfig> = emptyMap(),
    @EncodeDefault
    val connections: Map<String, ZincConnectionsConfig> = emptyMap(),
    @EncodeDefault
    val sources: Map<String, ZincSourceConfig> = emptyMap(),
)
@Serializable
data class ZincProcessorConfig(
    val type: String,
    @EncodeDefault
    val config: Map<String, String> = emptyMap()
)
@Serializable
data class ZincConnectionsConfig(
    @EncodeDefault
    val connections: Map<String, List<String>> = emptyMap()
)
@Serializable
data class ZincSourceConfig(
    val type: String,
    @EncodeDefault
    val config: Map<String, String> = emptyMap()
)

@Serializable
data class HConfigLayer<T>(val layerName: String, val config: T, val rawLayer: Config)

@Serializable
data class CMConfigLayer(val layerName: String, val config: Config, val rawConfig: Config)

@OptIn(ExperimentalSerializationApi::class)
object ConfigDefaults {
    // Generate the baseline Config object programmatically
    val defaultValuesConfig: Config by lazy {
        val defaultInstance = ZincConfig()

        // This encodes the data class directly into a Typesafe Config object
        Hocon.encodeToConfig(ZincConfig.serializer(), defaultInstance)
    }
}

data class LayeredConfigContainer(
    val layerName: String,
    val resolvedConfig: Config,   // The underlying fully resolved Config
    val rawConfig: Config,        // The original unresolved Config (protects environment variables)
    val parsedObject: ZincConfig  // The strongly typed Kotlin object model
)

@OptIn(ExperimentalSerializationApi::class)
object ZincConfigManager {

    val renderOptions: ConfigRenderOptions = ConfigRenderOptions.defaults()
        .setOriginComments(false)
        .setJson(false)
        .setFormatted(true)

    private fun loadConfig(layerName: String, rawLayer: Config): LayeredConfigContainer {
        val rootDefaults = Hocon.encodeToConfig(ZincConfig.serializer(), ZincConfig())
        val resolved = rawLayer.withFallback(rootDefaults).resolve()

        val parsedObject = Hocon.decodeFromConfig(ZincConfig.serializer(), resolved)

        return LayeredConfigContainer(layerName, resolved, rawLayer, parsedObject)
    }

    fun loadLayer(layerName: String, hoconString: String): LayeredConfigContainer {
        val rawLayer = ConfigFactory.parseString(hoconString)
        return loadConfig(layerName, rawLayer)
    }

    fun loadLayer(layerName: String, source: Path): LayeredConfigContainer {
        val rawLayer = ConfigFactory.parseFile(source.toFile())
        return loadConfig(layerName, rawLayer)
    }

    fun exportWithDefaults(container: LayeredConfigContainer): String {
        val fullStructureDefaults = Hocon.encodeToConfig(ZincConfig.serializer(), container.parsedObject)

        val exportableConfig = mergeConfigsCleanly(container.rawConfig.root(), fullStructureDefaults.root()).toConfig()

        return exportableConfig.root().render(renderOptions)
    }

    /**
     * Recursively merges the generated defaults into the raw config.
     * If a key exists in the raw config, we keep it exactly as-is (preserving ${USER}).
     * If it's a nested map/object, we recurse into it to find missing sub-keys (like `config = {}`).
     */
    private fun mergeConfigsCleanly(raw: ConfigObject, defaults: ConfigObject): ConfigObject {
        var merged = raw

        for ((key, defaultValue) in defaults) {
            if (!merged.containsKey(key)) {
                // If the raw config doesn't have the key at all, inject the default value safely
                merged = merged.withValue(key, defaultValue)
            } else {
                val rawValue = merged[key]
                // If BOTH are nested objects/maps, we need to recurse down to catch deep defaults
                if (rawValue is ConfigObject && defaultValue is ConfigObject) {
                    merged = merged.withValue(key, mergeConfigsCleanly(rawValue, defaultValue))
                }
                // Otherwise, the raw value wins completely (primitives, lists, unresolved variables),
                // preventing the dual-entry "unresolved merge" comment block!
            }
        }
        return merged
    }
}

@OptIn(ExperimentalSerializationApi::class)
object HoconConfigLoader {
    val renderOptions: ConfigRenderOptions = ConfigRenderOptions.defaults()
        .setOriginComments(false)
        .setJson(false)
    val encoder = Hocon {
        encodeDefaults = true
    }
    val parseOptions = ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF)

    inline fun <reified T> readLayer(layerName: String, source: Path): HConfigLayer<T> {
        val rawConfig = ConfigFactory.parseFile(source.toFile(), parseOptions)

        val unRaw = rawConfig.resolve(ConfigResolveOptions.noSystem().setAllowUnresolved(true))
        val parsedRaw = encoder.decodeFromConfig<T>(unRaw)
        val zConfig = encoder.decodeFromConfig<T>(rawConfig.resolve())
        return HConfigLayer(layerName, zConfig, encoder.encodeToConfig(parsedRaw))//rawConfig)
    }

    fun writeLayer(layerName: String, data: HConfigLayer<*>, target: Path) {
        println("Writing layer: $layerName")
        println("Config: ${data.rawLayer.root().render(renderOptions)}")
//        target.toFile().outputStream().use {
//            val c = encoder.decodeFromConfig<Config>(data.rawLayer.root())
//            val l = encoder.encodeToConfig(data.rawLayer.root())
//            val s = l.root()
//            it.write(l.root().toString().toByteArray())
//            it.write(l.root().render(renderOptions).encodeToByteArray())
//        }
    }
}

@OptIn(ExperimentalSerializationApi::class)
fun main() {
    val configFilePath = Path.of("/home/dayoun9/projects/github/zinc-flow/zinc-flow-kotlin/config.hocon")



//    val defaultConfig = Hocon.encodeToConfig(ZincConfig())
//    val cm = ConfigManager(defaultConfig)
//    val systemLayer = cm.loadLayer("system", configFilePath)
//    println(systemLayer)
//    println(cm.exportWithDefaults(systemLayer))
//    return


    val defaults = ConfigFactory.parseString("""
    database {
        poolSize = 10
        timeout = 30s
        tags = []
    }
""")

    val rawHoconInput = $$"""
    database {
        host = ${?DB_HOST}
        poolSize = 20
    }
"""


    val cm = ZincConfigManager
    val systemLayer = cm.loadLayer("system", configFilePath)
    val exportLayer= cm.exportWithDefaults(systemLayer)
    println(systemLayer)
    println(exportLayer)
    return









    val loader = HoconConfigLoader
    val layer = loader.readLayer<ZincConfig>("system", configFilePath)
    println("Config: ${layer.config}")
    loader.writeLayer("system", layer, configFilePath)
}
