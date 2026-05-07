package zincflow.fabric

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.InetAddress
import java.net.UnknownHostException
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import kotlin.collections.filterValues

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
class NodeIdentity(val nodeId: String, val hostname: String, val version: String) {
    private val bootMillis = System.currentTimeMillis()

    fun uptimeMillis(): Long {
        return System.currentTimeMillis() - bootMillis
    }

    /** Build an identity map suitable for JSON — the shape the UI sees
     * at `GET /api/identity` and on every registration POST. */
    fun toMap(port: Int): MutableMap<String, Any> {
        return buildMap {
            put("nodeId", nodeId)
            put("hostname", hostname)
            put("version", version)
            put("port", port)
            put("uptimeMillis", uptimeMillis())
            put("bootMillis", bootMillis)
        }.toMutableMap()
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(NodeIdentity::class.java)

        const val NODE_ID_FILE: String = "zincflow.nodeId"
        const val CONFIG_KEY: String = "ui.nodeId"

        /** Resolve identity at startup. */
        fun resolve(
            effectiveConfig: Map<String, Any>,
            nodeIdFile: Path,
            version: String
        ): NodeIdentity {
            val nodeId: String = readConfigNodeId(effectiveConfig) ?: loadOrCreatePersistedNodeId(nodeIdFile)
            val host: String = resolveHostname()
            return NodeIdentity(nodeId, host, version)
        }

        private fun readConfigNodeId(effectiveConfig: Map<String, Any>): String? {
            val ui = effectiveConfig["ui"]
            if (ui !is Map<*, *>) return null
            return (ui as Map<String, Any?>)["nodeId"]?.toString()
        }

        private fun loadOrCreatePersistedNodeId(path: Path): String {
            try {
                if (Files.exists(path)) {
                    val existing = Files.readString(path).trim()
                    if (existing.isNotEmpty()) return existing
                }
                val fresh = UUID.randomUUID().toString()
                Files.writeString(path, fresh)
                log.info("generated fresh node id: '$fresh' → '$path'")
                return fresh
            } catch (ex: IOException) {
                // Disk I/O failure shouldn't take the worker down. Fall back
                // to an ephemeral UUID — the operator can set ui.nodeId in
                // config to fix it properly.
                log.warn("node id persistence failed for '$path' ($ex) — using ephemeral id")
                return UUID.randomUUID().toString()
            }
        }

        private fun resolveHostname(): String {
            return try {
                InetAddress.getLocalHost().hostName
            } catch (_: UnknownHostException) {
                System.getenv("HOSTNAME") ?: System.getenv("COMPUTERNAME") ?: "unknown"
            }
        }
    }
}
