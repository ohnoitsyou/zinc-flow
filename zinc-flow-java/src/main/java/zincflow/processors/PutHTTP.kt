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
import java.io.IOException
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
    timeout: Duration? = Duration.ofSeconds(30),
    contentType: String? = "application/octet-stream",
    format: String? = "raw",
    store: ContentStore? = null
) : Processor {
    private val endpoint: URI
    private val method: String
    private val timeout: Duration?
    private val contentType: String
    private val v3: Boolean
    private val store: ContentStore?
    private val client: HttpClient

    init {
        require(!(endpoint == null || endpoint.isEmpty())) { "PutHTTP: endpoint must not be blank" }
        this.endpoint = URI.create(endpoint)
        this.method = when (method.uppercase(Locale.getDefault())) {
            "POST", "PUT", "PATCH" -> method.uppercase(Locale.getDefault())
            else -> throw IllegalArgumentException("PutHTTP: method must be POST/PUT/PATCH, got " + method)
        }
        this.timeout = if (timeout == null) Duration.ofSeconds(30) else timeout
        this.v3 = "v3".equals(format, ignoreCase = true)
        this.contentType = if (this.v3)
            "application/flowfile-v3"
        else
            (if (contentType == null) "application/octet-stream" else contentType)
        this.store = store
        this.client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .version(HttpClient.Version.HTTP_2)
            .build()
    }

    override fun process(ff: FlowFile): ProcessorResult {
        val resolved = ContentResolver.resolve(ff.content, store)
        if (!resolved.ok()) {
            return ProcessorResult.failure("PutHTTP: " + resolved.error, ff)
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
            if (status >= 200 && status < 300) {
                return ProcessorResult.single(withMeta)
            }
            return ProcessorResult.failure("PutHTTP: non-2xx status " + status, withMeta)
        } catch (ex: IOException) {
            if (ex is InterruptedException) {
                Thread.currentThread().interrupt()
            }
            log.warn("PutHTTP: transport error to {}: {}", endpoint, ex.toString())
            return ProcessorResult.failure("PutHTTP: " + ex.message, ff)
        } catch (ex: InterruptedException) {
            if (ex is InterruptedException) {
                Thread.currentThread().interrupt()
            }
            log.warn("PutHTTP: transport error to {}: {}", endpoint, ex.toString())
            return ProcessorResult.failure("PutHTTP: " + ex.message, ff)
        }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(PutHTTP::class.java)
    }
}
