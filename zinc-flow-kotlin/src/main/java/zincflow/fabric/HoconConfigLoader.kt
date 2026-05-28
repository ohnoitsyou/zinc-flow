package zincflow.fabric

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigObject
import com.typesafe.config.ConfigRenderOptions
import com.typesafe.config.ConfigValueType
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.hocon.Hocon
import zincflow.core.ProcessorContext
import java.nio.file.Path
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.iterator
import kotlin.io.path.exists
import kotlin.io.path.isDirectory
import kotlin.io.path.isRegularFile
import kotlin.io.path.walk
import kotlin.io.path.writeText

@Serializable
data class ZincConfig(
    val flow: ZincFlowConfig = ZincFlowConfig(),
)
@Serializable
data class ZincFlowConfig(
    val entryPoints: List<String> = emptyList(),
    val processors: Map<String, ZincProcessorConfig> = emptyMap(),
    val connections: Map<String, ZincConnectionsConfig> = emptyMap(),
    val sources: Map<String, ZincSourceConfig> = emptyMap(),
)
@Serializable
data class ZincProcessorConfig(
    val type: String,
    val config: Map<String, String> = emptyMap(),
)
@Serializable
data class ZincConnectionsConfig(
    val connections: Map<String, List<String>> = emptyMap(),
)
@Serializable
data class ZincSourceConfig(
    val type: String,
    val config: Map<String, String> = emptyMap(),
)

@JvmInline
value class UserConfigPath(val path: Path) : Path by path
@JvmInline
value class SystemConfigPath(val basePath: Path) : Path by basePath
@JvmInline
value class AdminConfigPath(val path: Path) : Path by path

data class SystemConfigLayer(
    val basePath: SystemConfigPath,
    val layers: List<Pair<Path, Config>> = emptyList(),
) {
    val paths by lazy { layers.map { it.first } }
}
data class UserConfigLayer(
    val path: UserConfigPath,
    val layer: Config,
)
data class AdminConfigLayer(
    val path: AdminConfigPath,
    val layer: Config,
)
data class SimpleConfigStack(
    val system: SystemConfigLayer,
    val user: UserConfigLayer,
    val admin: AdminConfigLayer? = null,
)
data class ResolvedConfig(
    val config: Config,
)
data class SlimZincConfig(
    val stack: SimpleConfigStack,
    val resolvedConfig: ResolvedConfig,
    val zincConfig: ZincConfig,
)

/**
 * ZincConfigLoader will handle the layering of
 *   1.  zero or more system layers, specified by an administrator as a base config;
 *   2.  a mutable user layer, useful for flow design;
 *   3.  and an authoritative admin layer, for specifying absolutes;
 *   into a LayeredZincConfig object that represents the useful state of the data flow
 * Configuration is specified using the HOCON configuration language--a super type of JSON
 * Changes can be applied to the user config by calling #applyDeltaAndReloadStack with a stack to modify,
 *   and the raw HOCON string representing the delta of the user layer.
 * The changes will be applied at the user layer and then a new fully resolved stack will be returned.
 * The updated user layer will be written to disk.
 */
@OptIn(ExperimentalSerializationApi::class)
object ZincConfigLoader {
    const val RESET_SENTINEL = "___RESET___"

    val rootDefaults = Hocon.encodeToConfig(ZincConfig.serializer(), ZincConfig())
    private val renderOptions = ConfigRenderOptions.defaults()
        .setOriginComments(false)
        .setJson(false)
        .setFormatted(true)

    private fun loadSystemLayers(systemLayerPath: Path): SystemConfigLayer {
        require(systemLayerPath.isDirectory()) { "Expecting system layers in directory; [$systemLayerPath] is not a directory. " }
        val layers = systemLayerPath.walk().sorted().associateWith { ConfigFactory.parseFile(it.toFile()) }.toList()
        return SystemConfigLayer(SystemConfigPath(systemLayerPath), layers)
    }

    private fun loadUserLayer(userConfigPath: Path): UserConfigLayer {
        require(userConfigPath.exists() && userConfigPath.isRegularFile()) { "User config provided [$userConfigPath] either doesn't exist or is not a regular file."}
        return UserConfigLayer(UserConfigPath(userConfigPath), ConfigFactory.parseFile(userConfigPath.toFile()))
    }

    /**
     * Initialize a configuration stack, but don't actually resolve anything. All the layers are raw and unevaluated.
     * Can be passed to ZincConfigLoader#resolveConfig(SimpleConfigStack) to finish resolution into a full LayeredZincConfig
     * @param systemLayerPath Path to a folder containing flow snippets to be used as a base configuration
     * @param userConfigPath Path to the file to use as the user layer. This file should be writable to support saving changes
     * @param adminConfigPath Optional path containing admin values to be layered on top of all other configs.
     */
    fun initializeConfigStack(systemLayerPath: Path, userConfigPath: Path, adminConfigPath: Path? = null): SimpleConfigStack {
        val system = loadSystemLayers(systemLayerPath)
        val user = loadUserLayer(userConfigPath)
        val admin = adminConfigPath?.let { AdminConfigLayer(AdminConfigPath(it), ConfigFactory.parseFile(it.toFile())) }
        return SimpleConfigStack(system, user, admin)
    }

    /**
     * Complete LayeredZincConfig resolution.
     * @param systemLayerPath Path to a folder containing flow snippets to be used as a base configuration
     * @param userConfigPath Path to the file to use as the user layer. This file should be writable to support saving changes
     * @param adminConfigPath Optional path containing admin values to be layered on top of all other configs.
     * @return a fully resolved [SlimZincConfig]
     */
    fun resolveConfig(systemLayerPath: Path, userConfigPath: Path, adminConfigPath: Path?): SlimZincConfig {
        val stack = initializeConfigStack(systemLayerPath, userConfigPath, adminConfigPath)
        return resolveConfig(stack)
    }

    fun SimpleConfigStack.resolve() {
        resolveConfig(this)
    }

    /**
     * Resolve a pre-created [SimpleConfigStack]
     * @param stack a [SimpleConfigStack] to resolve
     * @return a fully resolved [SlimZincConfig]
     */
    fun resolveConfig(stack: SimpleConfigStack): SlimZincConfig {
        // Build the system layers
        var mutableStack = stack.system.layers.fold(ConfigFactory.empty()) { acc, (_, value) -> value.withFallback(acc) }
        // Apply user layer
        mutableStack = stack.user.layer.withFallback(mutableStack)
        // Apply the admin layer -- if an admin layer is not provided, continue to use the user layer
        mutableStack = stack.admin?.layer?.withFallback(mutableStack) ?: mutableStack

        // resolve placeholders
        val resolveBeforeStrip = mutableStack.resolve()

        // Clean null entries
        val strippedConfig = sanitizeTree(resolveBeforeStrip.root(), dropNulls = true).toConfig()

        val finalConfig = strippedConfig.withFallback(rootDefaults)
        // Apply type-safe wrapper to config
        val resolvedConfig = ResolvedConfig(finalConfig)
        val parsedConfig = Hocon.decodeFromConfig(ZincConfig.serializer(), resolvedConfig.config)

        return SlimZincConfig(stack, resolvedConfig, parsedConfig)
    }

    /**
     * Integrates an incoming raw HOCON delta string safely, executing deletions
     * and modifications without tripping over unresolved substitutions.
     */
    fun applyDeltaAndReloadStack(
        baseContainer: SlimZincConfig,
        hoconDeltaString: String,
        commit: Boolean = true
    ): SlimZincConfig {
        val rawUser = ConfigFactory.parseFile(baseContainer.stack.user.path.toFile())
        val rawDelta = ConfigFactory.parseString(hoconDeltaString)

        // Apply diff the user and delta trees to make sure keys are resolved properly
        val resetLocalObject = sanitizeTree(rawUser.root(), rawDelta.root())
        // Sanitize the delta against itself to process [RESET_SENTINEL] keys
        val rawDeltaWithoutResets = sanitizeTree(rawDelta.root())

        // Apply local config to the delta to get a fully resolved user layer
        val newUserLayer = rawDeltaWithoutResets.withFallback(resetLocalObject.toConfig()).render(renderOptions)
        if (commit) {
            baseContainer.stack.user.path.writeText(newUserLayer)
        }

        // Read back in our layers, probably not totally necessary.
        // The only one that will have changed will be the user layer.
        val freshSystem = SystemConfigLayer(baseContainer.stack.system.basePath, baseContainer.stack.system.paths.associateWith { ConfigFactory.parseFile(it.toFile()) }.toList())
        val freshUser = UserConfigLayer(baseContainer.stack.user.path, ConfigFactory.parseString(newUserLayer))
        val freshAdmin = baseContainer.stack.admin?.path?.let { AdminConfigLayer(it, ConfigFactory.parseFile(it.toFile())) }

        return resolveConfig(SimpleConfigStack(freshSystem, freshUser, freshAdmin))
    }

    /**
     * Recursively cleanses a tree structure based on targeting signals.
     * - When checking a Delta tree: Drops targeted keys from the active file tree.
     * - When running solo: Purges its own values (like stripping out transient 'RESET' strings or 'NULL' elements).
     *
     * @param activeTree The target tree structure you are actively modifying or cleaning.
     * @param deltaTree The map structure guiding the operation. Defaults to 'activeTree' to run a self-clean pass.
     * @param dropNulls Set to true if you want to permanently erase HOCON NULL elements from memory.
     */
    private fun sanitizeTree(
        activeTree: ConfigObject,
        deltaTree: ConfigObject = activeTree,
        dropNulls: Boolean = false
    ): ConfigObject {
        var processedTree = activeTree

        for ((key, guideValue) in deltaTree) {
            val isResetToken = guideValue.valueType() == ConfigValueType.STRING && guideValue.unwrapped() == RESET_SENTINEL
            val isNullToken = dropNulls && guideValue.valueType() == ConfigValueType.NULL

            if (isResetToken || isNullToken) {
                // Drop the key entirely from the active tree map
                processedTree = processedTree.withoutKey(key)
            } else {
                val activeSubValue = processedTree[key]
                // If both trees share an inner map scope, step down recursively
                if (activeSubValue is ConfigObject && guideValue is ConfigObject) {
                    processedTree = processedTree.withValue(
                        key,
                        sanitizeTree(activeSubValue, guideValue, dropNulls)
                    )
                }
            }
        }
        return processedTree
    }
}

fun main() {
    val projectBase = Path.of("/home/dayoun9/projects/github/zinc-flow/zinc-flow-kotlin/zincConfig/base-2")
    val applicationConfig = ZincConfigLoader.resolveConfig(projectBase.resolve("base"), projectBase.resolve("config.local.conf"), projectBase.resolve("config.admin.conf"))
    println("Zinc Config: ${applicationConfig.zincConfig}")

    val p = ZincConfigProcessor(ProcessorRegistry(), SourceRegistry(), ProviderRegistry())
    p.toPipeline(applicationConfig.zincConfig, ProcessorContext())

//    val updatedConfig = ZincConfigLoader.applyDeltaAndReloadStack(applicationConfig, "flow { connections = ___RESET___ }", false)
//    println("Updated Config: ${updatedConfig.zincConfig}")
}