package zincflow
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigRenderOptions
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigValueType
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.hocon.Hocon
import zincflow.fabric.SlimZincConfig
import zincflow.fabric.ZincConfig
import zincflow.fabric.ZincConfigLoader
import java.nio.file.Path

object ZincConfigVisualizer {

    private val renderOptions = ConfigRenderOptions.defaults()
        .setOriginComments(false)
        .setJson(false)
        .setFormatted(false)

    @OptIn(ExperimentalSerializationApi::class)
    private val rootDefaults = Hocon.encodeToConfig(ZincConfig.serializer(), ZincConfig())

    fun visualizePathTrace(slimConfig: SlimZincConfig, targetPath: String) {
        val stack = slimConfig.stack

        println("==========================================================================================================")
        println(" CONFIGURATION PROGRESSIVE CASCADE TRACE FOR PATH: \"$targetPath\"")
        println("==========================================================================================================")
        println(String.format("%-24s | %-22s | %-10s | %s", "LAYER / PHASE", "SOURCE ORIGIN", "STATUS", "ROLLING VALUE STATE"))
        println("=========================+========================+============+==========================================")

        var runningCumulativeState = rootDefaults.resolve()

        // 1. Core Defaults Layer
        printLayerRow(
            layerTitle = "Code Defaults",
            sourceName = "Built-in Blueprint",
            rawLayerConfig = rootDefaults,
            previousCumulativeState = ConfigFactory.empty(),
            resolvedCumulativeState = runningCumulativeState,
            targetPath = targetPath
        )
        printDivider()

        // 2. Progressive System Layers
        stack.system.layers.forEachIndexed { index, (path, rawLayerConfig) ->
            val nextCumulativeState = rawLayerConfig.withFallback(runningCumulativeState).resolve()

            printLayerRow(
                layerTitle = "System Layer [${index + 1}]",
                sourceName = path.fileName.toString(),
                rawLayerConfig = rawLayerConfig,
                previousCumulativeState = runningCumulativeState,
                resolvedCumulativeState = nextCumulativeState,
                targetPath = targetPath
            )
            runningCumulativeState = nextCumulativeState
        }
        printDivider()

        // 3. User Local Layer Override
        val userCumulativeState = stack.user.layer.withFallback(runningCumulativeState).resolve()
        printLayerRow("User Local Layer", stack.user.path.fileName.toString(), stack.user.layer, runningCumulativeState, userCumulativeState, targetPath)
        printDivider()
        runningCumulativeState = userCumulativeState

        // 4. Secure Admin Override Constraint
        if (stack.admin != null) {
            val adminCumulativeState = stack.admin.layer.withFallback(runningCumulativeState).resolve()
            printLayerRow("Admin Boundary Layer", stack.admin.path.fileName.toString(), stack.admin.layer, runningCumulativeState, adminCumulativeState, targetPath)
            printDivider()
            runningCumulativeState = adminCumulativeState
        } else {
            println(String.format("%-24s | %-22s | %-10s | %s", "Admin Boundary Layer", "[Not Configured]", "-", "(Skipped)"))
            printDivider()
        }

        // 5. Final Active Combined Result Pass
        printFinalActiveRow(slimConfig.resolvedConfig.config, targetPath)
        println("==========================================================================================================")
    }

    private fun printLayerRow(
        layerTitle: String,
        sourceName: String,
        rawLayerConfig: Config,
        previousCumulativeState: Config,
        resolvedCumulativeState: Config,
        targetPath: String
    ) {
        val hasPathInThisLayer = rawLayerConfig.hasPathOrNull(targetPath)

        // FIX: Safely extract the wrapper token node to bypass the throwing getValue() method
        val currentTargetValue: ConfigValue? = getRawValueOrNull(resolvedCumulativeState, targetPath)
        val previousTargetValue: ConfigValue? = getRawValueOrNull(previousCumulativeState, targetPath)

        // Column C: Action Tracker
        val status = when {
            !hasPathInThisLayer -> "-"
            else -> {
                if (currentTargetValue == null || currentTargetValue.valueType() == ConfigValueType.NULL) {
                    "X [Null]"
                } else if (previousTargetValue == null || previousTargetValue.valueType() == ConfigValueType.NULL) {
                    "✓ [Added]"
                } else {
                    // Check actual value equality safely between layers
                    if (previousTargetValue.unwrapped() == currentTargetValue.unwrapped()) "-" else "✍ [Mod]"
                }
            }
        }

        // Column D: Rolling Value Stringifier
        val rollingValueStr = when {
            currentTargetValue == null -> "• [Undefined]"
            currentTargetValue.valueType() == ConfigValueType.NULL -> "Literal NULL (Erased Downstream Keys)"
            else -> currentTargetValue.render(renderOptions)
        }

        println(String.format("%-24s | %-22s | %-10s | %s", layerTitle, truncate(sourceName, 22), status, rollingValueStr))
    }

    private fun printFinalActiveRow(finalResolvedConfig: Config, targetPath: String) {
        val finalNode = getRawValueOrNull(finalResolvedConfig, targetPath)
        val activeValue = when {
            finalNode == null -> "UNDEFINED"
            finalNode.valueType() == ConfigValueType.NULL -> "EMPTY / NULL"
            else -> finalNode.render(renderOptions)
        }
        println(String.format("%-24s | %-22s | %-10s | ** %s **", "ACTIVE RUNTIME ENGINE", "Sanitized Object Memory", "READY", activeValue))
    }

    /**
     * Safely reads a path down a multi-level HOCON object map structure
     * without tripping over unexpected literal Null tokens.
     */
    private fun getRawValueOrNull(config: Config, path: String): ConfigValue? {
        if (!config.hasPathOrNull(path)) return null

        // Dive step by step into the root tree structure
        val parts = path.split(".")
        var currentObject = config.root()

        for (i in 0 until parts.size - 1) {
            val element = currentObject[parts[i]]
            if (element is com.typesafe.config.ConfigObject) {
                currentObject = element
            } else {
                return null // Path broken or cut off by a primitive/null higher up
            }
        }

        return currentObject[parts.last()]
    }

    private fun printDivider() {
        println("-------------------------+------------------------+------------+------------------------------------------")
    }

    private fun truncate(str: String, maxLength: Int): String {
        return if (str.length <= maxLength) str else str.substring(0, maxLength - 3) + "..."
    }
}

fun main() {
    val projectBase = Path.of("/home/dayoun9/projects/github/zinc-flow/zinc-flow-kotlin/zincConfig/base-2")
    val applicationConfig = ZincConfigLoader.resolveConfig(projectBase.resolve("base"), projectBase.resolve("config.local.conf"), projectBase.resolve("config.admin.conf"))
    ZincConfigVisualizer.visualizePathTrace(applicationConfig, "flow.connections")
//    val updatedConfig = ZincConfigLoader.applyDeltaAndReloadStack(applicationConfig, "flow { connections = ___RESET___ }", false)
//    ZincConfigVisualizer.visualizePathTrace(updatedConfig, "flow.processors")
}