package zincflow.fabric

import com.fasterxml.jackson.annotation.JsonCreator
import zincflow.providers.VersionControlProvider
import kotlin.io.encoding.Base64
import kotlin.time.Instant

data class ConfigLayer(val role: String?, val path: String?, val present: Boolean, val size: Int)
data class ConfigOverlayContainer(val base: String?, val layers: List<ConfigLayer>, val effective: Map<String, Any>, val provenance: Map<String, String>)

data class IngestResponse(val status: String, val flowfile: String, val target: String)

data class FlowResponse(
    val entryPoints: List<String>,
    val processors: List<FlowProcessorResponse>,
    val connections: Map<String, Map<String, List<String>>>,
    val providers: List<FlowProviderResponse>,
    val sources: List<FlowSourceResponse>,
    val stats: SummaryStats
)

data class FlowProcessorResponse(
    val name: String,
    val type: String,
    val state: String,
    val config: Map<String, String>,
    val stats: Map<String, Long>,
    val connections: Map<String, List<String>>
)

data class FlowSourceResponse(val name: String, val type: String, val running: Boolean)

data class FlowProviderResponse(val name: String, val type: String, val stats: String)

sealed class StatusResponse(val name: String) {
    class OKStatus(name: String, val status: String): StatusResponse(name)
    class ErrorStatus(name: String, val error: String): StatusResponse(name)
}

data class AddProcessorRequest(
    val name: String,
    val type: String,
    val config: Map<String, String> = emptyMap(),
    val requires: List<String> = emptyList(),
    val connections: Map<String, List<String>> = emptyMap()
)

data class RemoveProcessorRequest(val name: String)

data class EnableProcessorRequest(val name: String = "")

data class DisableProcessorRequest(val name: String)

data class ProcessorStateRequest(val name: String)

data class ProcessorStateResponse(val name: String, val state: String)

data class ProcessorSamplesRequest(val name: String = "")

data class SampleResponse(val timestamp: Instant, val flowfile: String, val contentType: String, val preview: String?, val previewBas64: String?, val attributes: Map<String, String>) {
    companion object {
        fun of(sample: SampleRegistry.SampleEntry): SampleResponse {
            return with(sample) {
                val b64 = if (contentType == "records") null else Base64.encode(preview)
                SampleResponse(timestamp, "ff-${flowfileId}", contentType, SampleRegistry.previewAsString(preview), b64, attributes)
            }
        }
    }
}

data class ProcessorSamplesResponse(val name: String, val sampling: Boolean, val samples: List<SampleResponse>)

data class LayoutResponse(val positions: Map<String, LayoutXY>, val path: String = "")

data class Layout(val positions: Map<String, LayoutXY>)

data class LayoutXY(val x: Double, val y: Double)

data class AddConnectionRequest(val from: String, val relationship: String, val to: String)

data class ConnectionModificationResponse(val status: String, val from: String, val relationship: String, val to: String) {
    fun toMap(): Map<String, String> {
        return mapOf("status" to status, "from" to from, "relationship" to relationship, "to" to to)
    }
}

data class ConnectionSetResponse(val status: String, val from: String, val relationships: Map<String, List<String>>) {
    fun toMap(): Map<String, Any> {
        return mapOf("status" to status, "from" to from, "relationships" to relationships)
    }
}

data class SetConnectionRequest @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(val targets: Map<String, List<String>>)

data class EntrypointModificationResponse(val status: String, val names: List<String>) {
    fun toMap(): Map<String, Any> {
        return mapOf("status" to status, "names" to names)
    }
}

data class VcCommandResponse(val ok: Boolean, val exitCode: Int, val stdout: String, val stderr: String) {
    companion object {
        /**
         * .toResponseObj is redundant as the CommandResult is basically the same. Going to leave for now though
         */
        fun VersionControlProvider.CommandResult.toResponseObj(): VcCommandResponse{
            return of(this)
        }

        fun of(result: VersionControlProvider.CommandResult): VcCommandResponse {
            return with(result) {
                VcCommandResponse(ok, exitCode, stdout, stderr)
            }
        }
    }
}

data class VcCommitRequest(val message: String, val path: String = "")

data class UpdateProcessorRequest(val type: String?, val config: Map<String, String>)

data class EditResultResponse(val status: String, val name: String) {
    fun toMap(): Map<String, Any> {
        return mapOf("status" to status, "name" to name)
    }
}

data class MetricSnapshot(val totalIngested: Long, val totalProcessed: Long, val totalDropped: Long, val totalFailed: Long, val processorCounts: Map<String, Long>, val processorErrors: Map<String, Long>)

data class SummaryStats(val processed: Long, val activeExecutions: Int, val processors: Int, val sources: Int)

data class RemoveConnectionRequest(val from: String, val relationship: String, val to: String)

data class SetEntrypointRequest(val names: List<String>)

data class VcPushRequest(val message: String?, val push: Boolean = true)

data class ConfigSourceSpec @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(val spec: Map<String, ConfigSpec>)
data class ConfigSpec(val type: String, val config: Map<String, String>)

data class ConfigurationRoot(val flow: RootConfig)
data class RootConfig(
    val entryPoints: List<String> = listOf(),
    val processors: Map<String, ConfigProcessorSpec> = mapOf(),
    val connections: Map<String, ConfigRelationship> = mapOf(),
    val sources: Map<String, SourceSpec> = mapOf()
)
data class SourceSpec(val type: String, val config: Map<String, Any> = mapOf())
data class ConfigRelationship @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(val relationships: Map<String, List<String>>)
data class ConfigProcessorSpec(val type: String, val config: Map<String, Any> = mapOf())