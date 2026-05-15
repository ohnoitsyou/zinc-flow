package zincflow.fabric

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.StreamReadFeature
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml
import zincflow.core.YamlMapper
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

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
    private val log: Logger = LoggerFactory.getLogger(ConfigOverlay::class.java)

    const val DEFAULT_LOCAL_NAME: String = "config.local.yaml"
    const val DEFAULT_SECRETS_NAME: String = "secrets.yaml"
    const val ENV_LOCAL: String = "ZINCFLOW_CONFIG_LOCAL"
    const val ENV_SECRETS: String = "ZINCFLOW_SECRETS_PATH"

    val yml = YAMLMapper().apply {
        registerKotlinModule()
        disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
        disable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES)
        disable(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES)
        setDefaultPropertyInclusion(JsonInclude.Value.ALL_NON_NULL)
        setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)
    }
    val yamlMapper: ObjectMapper = YamlMapper.mapper.copy()
        .disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
        .disable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES)
        .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)
    /** Explicit paths — used by tests and the admin API's
     * `PUT /api/overlays/secrets` write-through path. */
    /** Default behaviour — env vars first, sibling files as fallback. */
    @JvmStatic
    @JvmOverloads
    @Throws(IOException::class)
    fun load(
        basePath: Path,
        localPath: Path? = resolvePath(basePath, ENV_LOCAL, DEFAULT_LOCAL_NAME),
        secretsPath: Path? = resolvePath(basePath, ENV_SECRETS, DEFAULT_SECRETS_NAME)
    ): Resolved {
        val base = readLayer("base", basePath)
        val local = readLayer("local", localPath)
        val secrets = readLayer("secrets", secretsPath)

        val effective = mutableMapOf<String, Any>()
        val provenance = mutableMapOf<String, String>()

        merge(effective, provenance, base.content, base.role, "")
        merge(effective, provenance, local.content, local.role, "")
        merge(effective, provenance, secrets.content, secrets.role, "")

        return Resolved(basePath, mutableListOf(base, local, secrets), effective, provenance)
    }

    fun resolvePath(basePath: Path?, environmentKey: String, defaultKey: String) : Path {
        val envOverride = System.getenv(environmentKey)
        return when {
            envOverride != null && envOverride.isNotEmpty() -> Path.of(envOverride)
            basePath == null -> Path.of(defaultKey)
            else -> basePath.toAbsolutePath().parent.resolve(defaultKey)
        }
    }

    // I think this might be the root of the yaml parsing.
    @Throws(IOException::class)
    private fun readLayer(role: String, path: Path?): Layer {
        if (path == null || !Files.isRegularFile(path)) {
            return Layer(role, path, false, mapOf())
        }
        try {
            val fileLayer = yml.readValue(path.toFile(), ConfigurationRoot::class.java)
            println(fileLayer)
        } catch (e: Exception) {
            log.error("Exception parsing layer: $e")
        }
        val yaml = Files.readString(path)
        if (yaml.isBlank()) return Layer(role, path, true, mapOf())

        val parsedYaml = yamlMapper.readValue(yaml, Map::class.java)
        val processedYaml = if (parsedYaml is Map<*, *>) {
            parsedYaml.entries.associate { (k, v) -> k as String to v as Any }
        } else { emptyMap() }

        return Layer(role, path, true, processedYaml)
//        val parsed = Yaml().load<Any?>(yaml) ?: return Layer(role, path, true, mutableMapOf())
//        require(parsed is Map<*, *>) { "overlay '$role' ($path) must be a YAML map, got ${parsed.javaClass.getSimpleName()}" }
//        return Layer(role, path, true, normalizeKeys(parsed as Map<Any, Any>))
    }

    /** Recursive deep-merge: `src` onto `dst` with
     * dot-path provenance tracking into `provenance`. */
    private fun merge(
        dst: MutableMap<String, Any>,
        provenance: MutableMap<String, String>,
        src: Map<String, Any>,
        layerRole: String?,
        parentPath: String
    ) {
        if (src.isEmpty()) return
        for ((key, value) in src.entries) {
            val path = if (parentPath.isEmpty()) key else "$parentPath.$key"

            val existing = dst[key]
            if (existing is Map<*, *> && value is Map<*, *>) {
                // This feels inefficient, but if it's only done during load... it's probably fine
                val mergedChild = existing.entries
                    .associate { (key, value) -> key.toString() to (value as Any) }
                    .toMutableMap()
                val mappedValue = value.entries
                    .associate { (key, value) -> key.toString() to (value as Any) }

                merge(mergedChild, provenance, mappedValue, layerRole, path)
                dst[key] = mergedChild
            } else {
                dst[key] = deepCopyValue(value)
                provenance[path] = layerRole ?: ""
            }
        }
    }

    private fun normalizeKeys(raw: Map<Any, Any>): Map<String, Any> {
        return raw.entries.associate { (key, value) ->
            key.toString() to (value.takeUnless { value is Map<*, *> } ?: normalizeKeys(value as Map<Any, Any>))
        }
    }

    private fun deepCopy(src: Map<String, Any>): Map<String, Any> {
        return src.entries.associate { (key, value) -> key to deepCopyValue(value) }
    }

    private fun deepCopyValue(v: Any): Any {
        return when (v) {
            is Map<*, *> -> deepCopy(v as Map<String, Any>)
            is List<*> -> v.map { deepCopyValue(it as Any) }
            else -> v
        }
    }

    /** A single layer in the overlay stack, with where the bytes came
     * from, the role (base / local / secrets), and whether the file
     * was actually present. */
    @ConsistentCopyVisibility
    data class Layer private constructor(@JvmField val role: String?, val path: Path?, @JvmField val present: Boolean, val content: Map<String, Any> = mapOf()) {
        companion object {
            operator fun invoke(role: String?, path: Path?, present: Boolean, content: Map<String, Any> = mapOf()): Layer {
                return Layer(role, path, present, deepCopy(content))
            }
        }
    }

    /** Result of a [.load] call: every layer, the merged map,
     * and per-dot-path provenance. */
    data class Resolved(
        val basePath: Path?,
        val layers: MutableList<Layer> = mutableListOf(),
        val effective: MutableMap<String, Any> = mutableMapOf(),
        val provenance: MutableMap<String, String> = mutableMapOf(),
    ) { }
}
