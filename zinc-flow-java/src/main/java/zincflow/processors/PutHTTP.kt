package zincflow.processors

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import zincflow.core.ContentResolver
import zincflow.core.ContentStore
import zincflow.core.FlowFile
import zincflow.core.FlowFileAttributes
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.fabric.FlowFileV3
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse.BodyHandlers
import java.time.Duration
import java.util.Locale

/** Sink-style processor that POSTs / PUTs / PATCHes the FlowFile's
 * content to a URL via the JDK's [HttpClient]. 2xx → success;
 * non-2xx or transport error → failure.
 * 
 * `format=v3` wraps the body with NiFi FlowFile V3 framing and
 * sets `application/flowfile-v3` as the Content-Type so the
 * downstream receiver can round-trip the original attributes.
 * Claim-backed content resolves through the supplied
 * [ContentStore]. */
class PutHTTP @JvmOverloads constructor(
    endpoint: String,
    method: String = "POST",
    private val timeout: Duration = Duration.ofSeconds(30),
    contentType: String? = "application/octet-stream",
    format: String? = "raw",
    private val store: ContentStore? = null
) : Processor {
    private val endpoint: URI = endpoint.takeIf { it.isNotBlank() }
        ?.let { URI.create(it) }
        ?: throw IllegalArgumentException("PutHTTP: endpoint must not be blank")

    private val method: String = when (method.uppercase(Locale.getDefault())) {
        "POST", "PUT", "PATCH" -> method.uppercase(Locale.getDefault())
        else -> throw IllegalArgumentException("PutHTTP: method must be POST/PUT/PATCH, got $method")
    }

    private val v3: Boolean = "v3".equals(format, ignoreCase = true)
    private val contentType: String = if (this.v3) {
        "application/flowfile-v3"
    } else {
        contentType ?: "application/octet-stream"
    }

    private val client: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .version(HttpClient.Version.HTTP_2)
        .build()

    override fun process(ff: FlowFile): ProcessorResult {
        val resolved = ContentResolver.resolve(ff.content, store)
        if (!resolved.ok()) {
            return ProcessorResult.Failure("PutHTTP: " + resolved.error, ff)
        }
        val body = if (v3) FlowFileV3.pack(ff, resolved.bytes) else resolved.bytes
        val request = HttpRequest.newBuilder()
            .uri(endpoint)
            .timeout(timeout)
            .header("Content-Type", contentType)
            .method(method, BodyPublishers.ofByteArray(body))
            .build()
        try {
            val response = client.send<ByteArray?>(request, BodyHandlers.ofByteArray())
            val status = response.statusCode()
            val withMeta = ff
                .withAttribute(FlowFileAttributes.PUTHTTP_STATUS, status.toString())
                .withAttribute(FlowFileAttributes.PUTHTTP_RESPONSE_SIZE, response.body()!!.size.toString())
            if (status in 200..<300) {
                return ProcessorResult.Single(withMeta)
            }
            return ProcessorResult.Failure("PutHTTP: non-2xx status $status", withMeta)
        } catch (ex: Exception) {
            Thread.currentThread().interrupt()
            log.warn("PutHTTP: transport error to {}: {}", endpoint, ex.toString())
            return ProcessorResult.Failure("PutHTTP: " + ex.message, ff)
        }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(PutHTTP::class.java)
    }
}
