package zincflow.fabric

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.InetAddress
import java.net.UnknownHostException
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID

/** Resolves this worker's stable identity. Returned map shape is what
 * the management API emits at `GET /api/identity` and what the
 * [zincflow.providers.UIRegistrationProvider] sends to the UI.
 * 
 * <h2>Node id resolution</h2>
 * 
 *  1. `ui.nodeId` in the effective layered config — explicit
 * operator override. Stable across restarts by construction.
 *  1. UUID persisted to `./zincflow.nodeId` on first boot.
 * Survives restarts when that path lives on a volume;
 * regenerates otherwise.
 * 
 * 
 * Hostname always comes from [InetAddress.getLocalHost] and
 * is kept distinct from nodeId — a k8s pod rename rotates hostname
 * but not the logical identity. */
class NodeIdentity(private val nodeId: String?, private val hostname: String?, private val version: String?) {
    private val bootMillis = System.currentTimeMillis()

    fun nodeId(): String? {
        return nodeId
    }

    fun hostname(): String? {
        return hostname
    }

    fun version(): String? {
        return version
    }

    fun uptimeMillis(): Long {
        return System.currentTimeMillis() - bootMillis
    }

    /** Build an identity map suitable for JSON — the shape the UI sees
     * at `GET /api/identity` and on every registration POST. */
    fun toMap(port: Int): MutableMap<String?, Any?> {
        val out: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        out.put("nodeId", nodeId)
        out.put("hostname", hostname)
        out.put("version", version)
        out.put("port", port)
        out.put("uptimeMillis", uptimeMillis())
        out.put("bootMillis", bootMillis)
        return out
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(NodeIdentity::class.java)

        const val NODE_ID_FILE: String = "zincflow.nodeId"
        const val CONFIG_KEY: String = "ui.nodeId"

        /** Resolve identity at startup. */
        fun resolve(
            effectiveConfig: MutableMap<String?, Any?>?,
            nodeIdFile: Path?,
            version: String?
        ): NodeIdentity {
            val configured: String? = readConfigNodeId(effectiveConfig)
            val nodeId: String? = if (configured != null) configured else loadOrCreatePersistedNodeId(nodeIdFile)
            val host: String? = resolveHostname()
            return NodeIdentity(nodeId, host, if (version == null) "unknown" else version)
        }

        private fun readConfigNodeId(effectiveConfig: MutableMap<String?, Any?>?): String? {
            if (effectiveConfig == null) return null
            val ui = effectiveConfig.get("ui")
            if (ui !is MutableMap<*, *>) return null
            val v = (ui as MutableMap<String?, Any?>).get("nodeId")
            return if (v == null) null else v.toString()
        }

        private fun loadOrCreatePersistedNodeId(path: Path?): String {
            var path = path
            if (path == null) path = Path.of(NODE_ID_FILE)
            try {
                if (Files.exists(path)) {
                    val existing = Files.readString(path).trim { it <= ' ' }
                    if (!existing.isEmpty()) return existing
                }
                val fresh = UUID.randomUUID().toString()
                Files.writeString(path, fresh)
                log.info("generated fresh node id {} → {}", fresh, path)
                return fresh
            } catch (ex: IOException) {
                // Disk I/O failure shouldn't take the worker down. Fall back
                // to an ephemeral UUID — the operator can set ui.nodeId in
                // config to fix it properly.
                log.warn("node id persistence failed for {} ({}) — using ephemeral id", path, ex.toString())
                return UUID.randomUUID().toString()
            }
        }

        private fun resolveHostname(): String? {
            try {
                return InetAddress.getLocalHost().getHostName()
            } catch (ex: UnknownHostException) {
                val env = System.getenv("HOSTNAME")
                return if (env == null) "unknown" else env
            }
        }
    }
}
