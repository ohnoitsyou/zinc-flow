package zincflow.fabric

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.List
import java.util.Map

/** Loads a base config plus optional local / secrets overlays and
 * deep-merges them into a single effective config. Tracks which
 * layer supplied each dot-path so `GET /api/overlays` can
 * report provenance.
 * 
 * Layer order (later wins): base ← local ← secrets.
 * 
 * Overlay paths come from (in order):
 * 1. Explicit constructor argument (used by tests + programmatic callers)
 * 2. Environment variable (`$ZINCFLOW_CONFIG_LOCAL`, `$ZINCFLOW_SECRETS_PATH`)
 * 3. Sibling of the base config: `baseDir/config.local.yaml`,
 * `baseDir/secrets.yaml`
 * 
 * A missing file at any layer is not an error — the layer simply
 * contributes an empty map.
 * 
 * The `secrets` layer is read-only legacy: the on-disk write
 * endpoint was retired in favor of environment variables. The read
 * path is kept so existing `secrets.yaml` files continue to
 * merge, but no new secrets.yaml files are produced by the worker. */
object ConfigOverlay {
    private val log: Logger? = LoggerFactory.getLogger(ConfigOverlay::class.java)

    const val DEFAULT_LOCAL_NAME: String = "config.local.yaml"
    const val DEFAULT_SECRETS_NAME: String = "secrets.yaml"
    const val ENV_LOCAL: String = "ZINCFLOW_CONFIG_LOCAL"
    const val ENV_SECRETS: String = "ZINCFLOW_SECRETS_PATH"

    /** Explicit paths — used by tests and the admin API's
     * `PUT /api/overlays/secrets` write-through path. */
    /** Default behaviour — env vars first, sibling files as fallback. */
    @JvmStatic
    @JvmOverloads
    @Throws(IOException::class)
    fun load(
        basePath: Path?,
        localPath: Path? = resolveLocalPath(basePath),
        secretsPath: Path? = resolveSecretsPath(basePath)
    ): Resolved {
        val base = readLayer("base", basePath)
        val local = readLayer("local", localPath)
        val secrets = readLayer("secrets", secretsPath)

        val effective: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        val provenance: MutableMap<String?, String?> = LinkedHashMap<String?, String?>()
        merge(effective, provenance, base.content, base.role, "")
        merge(effective, provenance, local.content, local.role, "")
        merge(effective, provenance, secrets.content, secrets.role, "")
        return Resolved(basePath, List.of<Layer?>(base, local, secrets), effective, provenance)
    }

    fun resolveLocalPath(basePath: Path?): Path {
        val envOverride = System.getenv(ENV_LOCAL)
        if (envOverride != null && !envOverride.isEmpty()) return Path.of(envOverride)
        return if (basePath == null)
            Path.of(DEFAULT_LOCAL_NAME)
        else
            basePath.toAbsolutePath().getParent().resolve(DEFAULT_LOCAL_NAME)
    }

    fun resolveSecretsPath(basePath: Path?): Path {
        val envOverride = System.getenv(ENV_SECRETS)
        if (envOverride != null && !envOverride.isEmpty()) return Path.of(envOverride)
        return if (basePath == null)
            Path.of(DEFAULT_SECRETS_NAME)
        else
            basePath.toAbsolutePath().getParent().resolve(DEFAULT_SECRETS_NAME)
    }

    @Throws(IOException::class)
    private fun readLayer(role: String?, path: Path?): Layer {
        if (path == null || !Files.isRegularFile(path)) {
            return Layer(role, path, false, Map.of<String?, Any?>())
        }
        val yaml = Files.readString(path)
        if (yaml.isBlank()) return Layer(role, path, true, Map.of<String?, Any?>())
        val parsed = Yaml().load<Any?>(yaml)
        if (parsed == null) return Layer(role, path, true, Map.of<String?, Any?>())
        require(parsed is MutableMap<*, *>) { "overlay '" + role + "' (" + path + ") must be a YAML map, got " + parsed.javaClass.getSimpleName() }
        return Layer(role, path, true, normalizeKeys(parsed as MutableMap<Any?, Any?>))
    }

    /** Recursive deep-merge: `src` onto `dst` with
     * dot-path provenance tracking into `provenance`. */
    private fun merge(
        dst: MutableMap<String?, Any?>,
        provenance: MutableMap<String?, String?>,
        src: MutableMap<String?, Any?>?,
        layerRole: String?,
        parentPath: String
    ) {
        if (src == null || src.isEmpty()) return
        for (entry in src.entries) {
            val key: String = entry.key!!
            val value = entry.value
            val path = if (parentPath.isEmpty()) key else parentPath + "." + key

            val existing = dst.get(key)
            if (existing is MutableMap<*, *>
                && value is MutableMap<*, *>
            ) {
                val mergedChild: MutableMap<String?, Any?> =
                    LinkedHashMap<String?, Any?>(existing as MutableMap<String?, Any?>)
                merge(mergedChild, provenance, value as MutableMap<String?, Any?>, layerRole, path)
                dst.put(key, mergedChild)
            } else {
                dst.put(key, deepCopyValue(value))
                provenance.put(path, layerRole)
            }
        }
    }

    private fun normalizeKeys(raw: MutableMap<Any?, Any?>): MutableMap<String?, Any?> {
        val out: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        for (entry in raw.entries) {
            val v = entry.value
            val normalized = if (v is MutableMap<*, *>)
                normalizeKeys(v as MutableMap<Any?, Any?>)
            else
                v
            out.put(entry.key.toString(), normalized)
        }
        return out
    }

    private fun deepCopy(src: MutableMap<String?, Any?>): MutableMap<String?, Any?> {
        val out: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        for (entry in src.entries) {
            out.put(entry.key, deepCopyValue(entry.value))
        }
        return out
    }

    private fun deepCopyValue(v: Any?): Any? {
        if (v is MutableMap<*, *>) return deepCopy(v as MutableMap<String?, Any?>)
        if (v is MutableList<*>) {
            val out: MutableList<Any?> = ArrayList<Any?>(v.size)
            for (o in v) out.add(deepCopyValue(o))
            return out
        }
        return v
    }

    /** A single layer in the overlay stack, with where the bytes came
     * from, the role (base / local / secrets), and whether the file
     * was actually present. */
    class Layer(@JvmField val role: String?, val path: Path?, @JvmField val present: Boolean, content: MutableMap<String?, Any?>?) {
        val content: MutableMap<String?, Any?>?

        init {
            var content = content
            content = if (content == null) Map.of<String?, Any?>() else deepCopy(content)
            this.content = content
        }
    }

    /** Result of a [.load] call: every layer, the merged map,
     * and per-dot-path provenance. */
    class Resolved(
        val basePath: Path?,
        layers: MutableList<Layer?>?,
        effective: MutableMap<String?, Any?>?,
        provenance: MutableMap<String?, String?>?
    ) {
        val layers: MutableList<Layer?>?
        val effective: MutableMap<String?, Any?>?
        val provenance: MutableMap<String?, String?>?

        init {
            var layers = layers
            var effective = effective
            var provenance = provenance
            layers = List.copyOf<Layer?>(layers)
            effective = ConfigOverlay.deepCopy(effective!!)
            provenance = Map.copyOf<String?, String?>(provenance)
            this.layers = layers
            this.effective = effective
            this.provenance = provenance
        }
    }
}
