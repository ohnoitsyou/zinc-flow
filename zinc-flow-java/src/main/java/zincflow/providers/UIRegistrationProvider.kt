package zincflow.providers

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import zincflow.core.ComponentState
import zincflow.core.Provider
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.function.Supplier
import kotlin.concurrent.Volatile

/** Opt-in worker-side hook that registers this node with a central
 * UI binary and heartbeats every 30 s. The UI aggregates registered
 * workers into its node registry; self-registration is one of the
 * two supported discovery modes (static list is the other).
 * 
 * Config in `config.yaml`:
 * <pre>
 * ui:
 * register_to: http://zinc-flow-ui.ns.svc.cluster.local:9090
</pre> * 
 * 
 * The provider's lifecycle controls the heartbeat: `enable()`
 * starts the scheduled task (fire-once-immediately + every 30 s
 * afterward), `disable()` / `shutdown()` cancel it. */
class UIRegistrationProvider @JvmOverloads constructor(
    targetUrl: String,
    identitySupplier: Supplier<MutableMap<String?, Any?>?>,
    heartbeatSeconds: Long = DEFAULT_HEARTBEAT_SECONDS,
    http: HttpClient = HttpClient.newHttpClient()
) : Provider {
    private val targetUrl: String
    private val identitySupplier: Supplier<MutableMap<String?, Any?>?>
    private val heartbeatSeconds: Long
    private val json = ObjectMapper()
    private val http: HttpClient

    private var scheduler: ScheduledExecutorService? = null
    private var heartbeat: ScheduledFuture<*>? = null

    @Volatile
    private var state = ComponentState.DISABLED

    @Volatile
    private var lastHeartbeatMillis: Long = 0

    @Volatile
    private var lastStatus = -1

    @Volatile
    private var lastError: String? = ""

    init {
        require(!(targetUrl == null || targetUrl.isEmpty())) { "targetUrl must not be blank" }
        requireNotNull(identitySupplier) { "identitySupplier must not be null" }
        require(heartbeatSeconds > 0) { "heartbeatSeconds must be > 0" }
        this.targetUrl = targetUrl
        this.identitySupplier = identitySupplier
        this.heartbeatSeconds = heartbeatSeconds
        this.http = http
    }

    override fun name(): String {
        return NAME
    }

    override fun providerType(): String {
        return TYPE
    }

    override fun state(): ComponentState {
        return state
    }

    @Synchronized
    override fun enable() {
        if (state == ComponentState.ENABLED) return
        // Scheduler itself stays on a single platform daemon thread —
        // ScheduledThreadPoolExecutor's timing relies on park/unpark and
        // isn't virtual-thread friendly. Each tick's body runs on a
        // fresh virtual thread, so an unresponsive UI (5s timeout) can't
        // delay the next heartbeat.
        scheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("zinc-flow-ui-registration").factory()
        )
        heartbeat = scheduler!!.scheduleAtFixedRate(
            Runnable { Thread.startVirtualThread(Runnable { this.postOnce() }) },
            0, heartbeatSeconds, TimeUnit.SECONDS
        )
        state = ComponentState.ENABLED
        log.info("ui registration enabled — target {}, heartbeat {}s", targetUrl, heartbeatSeconds)
    }

    @Synchronized
    override fun disable(drainTimeoutSeconds: Int) {
        stopScheduler()
        state = ComponentState.DISABLED
    }

    override fun shutdown() {
        disable(0)
    }

    private fun stopScheduler() {
        if (heartbeat != null) {
            heartbeat!!.cancel(false)
            heartbeat = null
        }
        if (scheduler != null) {
            scheduler!!.shutdown()
            scheduler = null
        }
    }

    /** Exposed for tests + admin endpoints — most recent POST result. */
    fun lastOutcome(): MutableMap<String?, Any?> {
        val out: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        out.put("target", targetUrl)
        out.put("lastHeartbeatMillis", lastHeartbeatMillis)
        out.put("lastStatus", lastStatus)
        out.put("lastError", lastError)
        return out
    }

    private fun postOnce() {
        val identity: MutableMap<String?, Any?>?
        try {
            identity = identitySupplier.get()
        } catch (ex: RuntimeException) {
            lastError = "identity supplier threw: " + ex
            log.warn("ui registration: identity supplier threw {}", ex.toString())
            return
        }
        try {
            val req = HttpRequest.newBuilder(URI.create(targetUrl))
                .timeout(Duration.ofSeconds(5))
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(json.writeValueAsBytes(identity)))
                .build()
            val resp = http.send<String?>(req, HttpResponse.BodyHandlers.ofString())
            lastHeartbeatMillis = System.currentTimeMillis()
            lastStatus = resp.statusCode()
            lastError = ""
            if (resp.statusCode() >= 400) {
                log.warn("ui registration: {} returned {}", targetUrl, resp.statusCode())
            } else {
                log.debug("ui registration: heartbeat ok ({})", resp.statusCode())
            }
        } catch (ex: Exception) {
            lastHeartbeatMillis = System.currentTimeMillis()
            lastStatus = -1
            lastError = ex.toString()
            if (ex is InterruptedException) Thread.currentThread().interrupt()
            log.warn("ui registration: heartbeat to {} failed: {}", targetUrl, ex.toString())
        }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(UIRegistrationProvider::class.java)

        const val NAME: String = "ui_registration"
        const val TYPE: String = "UIRegistrationProvider"
        const val DEFAULT_HEARTBEAT_SECONDS: Long = 30
    }
}
