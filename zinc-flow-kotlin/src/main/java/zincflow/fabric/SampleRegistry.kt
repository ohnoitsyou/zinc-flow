package zincflow.fabric

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.commons.collections4.queue.CircularFifoQueue
import zincflow.core.ClaimContent
import zincflow.core.FlowFile
import zincflow.core.RawContent
import zincflow.core.RecordContent
import java.io.UnsupportedEncodingException
import java.nio.charset.Charset
import kotlin.time.Clock
import kotlin.time.Instant

object SampleRegistry {
    private val samples = HashMap<String, SampleBuffer>()
    var samplingEnabled: Boolean = true
        private set
    private val mapper = jacksonObjectMapper()
    const val SAMPLE_PREVIEW_BYTES = 4096

    fun pushSample(name: String, flowfile: FlowFile) {
        if(!samplingEnabled) return
        val buffer = samples.computeIfAbsent(name) { SampleBuffer(name) }

        val content = flowfile.content
        val contentType: String
        val preview = when(content) {
            is RawContent -> {
                contentType = "bytes"
                val length = minOf(content.size(), SAMPLE_PREVIEW_BYTES)
                content.bytes.copyOfRange(0, length)
            }
            is ClaimContent -> {
                contentType = "claim"
                "(claim ${content.claimId}, ${content.size} bytes)".toByteArray()
            }
            is RecordContent -> {
                contentType = "records"
                serializeRecordPreview(content)
            }
            else -> {
                contentType = "unknown"
                "".toByteArray()
            }
        }

        buffer.push(SampleEntry(Clock.System.now(), flowfile.id, contentType, preview, flowfile.attributes))
    }

    fun getSnapshotFor(name: String): List<SampleEntry> {
        return samples[name]?.snapshot() ?: emptyList()
    }

    fun previewAsString(content: ByteArray): String? {
        if (content.isEmpty()) return ""
        return try {
            content.toString(Charset.forName("utf-8"))
        } catch (_: UnsupportedEncodingException) {
            null
        }
    }

    private fun serializeRecordPreview(content: RecordContent): ByteArray {
        if(content.records.isEmpty()) return ByteArray(0)
        val record = content.records.first()
        val bytes = mapper.writeValueAsBytes(record)
        return bytes.copyOfRange(0, minOf(content.size(), SAMPLE_PREVIEW_BYTES))
    }

    data class SampleBuffer(val name: String) {
        private val samples =  CircularFifoQueue<SampleEntry>()

        fun push(entry: SampleEntry) {
            samples.add(entry)
        }

        fun snapshot(): List<SampleEntry> {
            return samples.asIterable().reversed().toList()
        }
    }

    data class SampleEntry(val timestamp: Instant, val flowfileId: Long, val contentType: String, val preview: ByteArray, val attributes: Map<String, String>) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as SampleEntry

            if (flowfileId != other.flowfileId) return false
            if (timestamp != other.timestamp) return false
            if (contentType != other.contentType) return false
            if (!preview.contentEquals(other.preview)) return false

            return true
        }

        override fun hashCode(): Int {
            var result = flowfileId.hashCode()
            result = 31 * result + timestamp.hashCode()
            result = 31 * result + contentType.hashCode()
            result = 31 * result + preview.contentHashCode()
            return result
        }
    }
}
