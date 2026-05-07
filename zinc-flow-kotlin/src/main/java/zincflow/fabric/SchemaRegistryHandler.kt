package zincflow.fabric

import com.fasterxml.jackson.databind.ObjectMapper
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.Handler
import zincflow.providers.SchemaRegistryProvider
import java.util.Map
import java.util.Optional
import java.util.function.Function

/** Confluent-shape REST surface over [SchemaRegistryProvider].
 * Mounted under `/api/schema-registry/ *` alongside the rest of
 * the management API so serializer clients that speak to Confluent
 * Schema Registry work verbatim against an embedded zinc-flow-java.
 * 
 * <h2>Mapped routes</h2>
 * <pre>
 * GET    /schemas/ids/{id}
 * GET    /subjects
 * GET    /subjects/{subject}/versions
 * GET    /subjects/{subject}/versions/{version|latest}
 * POST   /subjects/{subject}/versions          body: {"schema":"..."}
 * DELETE /subjects/{subject}
 * DELETE /subjects/{subject}/versions/{version}
</pre> * 
 * 
 * Responses carry
 * `Content-Type: application/vnd.schemaregistry.v1+json`, and
 * 4xx/5xx bodies match Confluent's `{"error_code", "message"}`
 * shape so any existing tooling that handles those errors works
 * against us unchanged. Mirrors zinc-flow-csharp's
 * `SchemaRegistryHandler`. */
class SchemaRegistryHandler(val registry: SchemaRegistryProvider) {
    private val json = ObjectMapper()

    fun mapRoutes(app: Javalin) {
        app.get("/api/schema-registry/schemas/ids/{id}") { this.getById(it) }
        app.get("/api/schema-registry/subjects") { this.listSubjects(it) }
        app.get("/api/schema-registry/subjects/{subject}/versions") { this.listVersions(it) }
        app.get("/api/schema-registry/subjects/{subject}/versions/{version}") { this.getVersion(it) }
        app.post("/api/schema-registry/subjects/{subject}/versions") { this.register(it) }
        app.delete("/api/schema-registry/subjects/{subject}") { this.deleteSubject(it) }
        app.delete("/api/schema-registry/subjects/{subject}/versions/{version}") { this.deleteVersion(it) }
    }

    // --- GET /schemas/ids/{id} → {"schema":"..."} ---
    @Throws(Exception::class)
    private fun getById(ctx: Context) {
        val id: Int
        try {
            id = Integer.parseInt(ctx.pathParam("id"))
        } catch (e: NumberFormatException) {
            error(ctx, 400, 40000, "id must be an integer")
            return
        }
        val entry = registry.getById(id)
        if (entry.isEmpty()) {
            error(ctx, 404, 40403, "schema id $id not found")
            return
        }
        write(ctx, 200, Map.of<String?, String?>("schema", entry.get().definition))
    }

    // --- GET /subjects → ["a","b","c"] ---
    @Throws(Exception::class)
    private fun listSubjects(ctx: Context) {
        writeRaw(ctx, 200, json.writeValueAsBytes(registry.listSubjects()))
    }

    // --- GET /subjects/{subject}/versions → [1,2,3] ---
    @Throws(Exception::class)
    private fun listVersions(ctx: Context) {
        val subject = ctx.pathParam("subject")
        val versions = registry.listVersions(subject)
        if (versions.isEmpty()) {
            error(ctx, 404, 40401, "subject '$subject' not found")
            return
        }
        writeRaw(ctx, 200, json.writeValueAsBytes(versions))
    }

    // --- GET /subjects/{subject}/versions/{version|latest} → full payload ---
    @Throws(Exception::class)
    private fun getVersion(ctx: Context) {
        val subject = ctx.pathParam("subject")
        val version = ctx.pathParam("version")
        val entry = if ("latest" == version)
            registry.latest(subject)
        else
            parseVersion(version).flatMap<SchemaRegistryProvider.Schema> { v: Int ->
                registry.getEntry(subject, v)
            }
        if (entry.isEmpty()) {
            // Distinguish unknown subject vs unknown version — Confluent does.
            if (!registry.listSubjects().contains(subject)) {
                error(ctx, 404, 40401, "subject '$subject' not found")
            } else {
                error(ctx, 404, 40402, "subject '$subject' version $version not found")
            }
            return
        }
        val s = entry.get()
        val body = buildMap {
            put("subject", s.subject ?: "")
            put("version", s.version)
            put("id", s.id)
            put("schema", s.definition ?: "")
        }
        write(ctx, 200, body)
    }

    private fun parseVersion(raw: String): Optional<Int> {
        return try {
            Optional.of<Int>(Integer.parseInt(raw))
        } catch (_: NumberFormatException) {
            Optional.empty<Int>()
        }
    }

    // --- POST /subjects/{subject}/versions  body {"schema":"..."}  → {"id":N} ---
    @Throws(Exception::class)
    private fun register(ctx: Context) {
        val subject = ctx.pathParam("subject")
        val body = ctx.body()
        val schemaDef: String
        try {
            val root = json.readTree(body)
            val schemaNode = root.get("schema")
            if (schemaNode == null || !schemaNode.isTextual()) {
                error(ctx, 422, 42201, "request body missing 'schema' string field")
                return
            }
            schemaDef = schemaNode.asText()
        } catch (ex: Exception) {
            error(ctx, 422, 42201, "request body is not valid JSON: " + ex.message)
            return
        }

        try {
            val registered = registry.register(subject, schemaDef)
            write(ctx, 200, Map.of<String?, Int?>("id", registered.id))
        } catch (ex: IllegalArgumentException) {
            error(ctx, 422, 42202, "register failed: " + ex.message)
        } catch (ex: RuntimeException) {
            error(ctx, 500, 50001, "register failed: " + ex.message)
        }
    }

    // --- DELETE /subjects/{subject} → [1,2,3] of removed versions ---
    @Throws(Exception::class)
    private fun deleteSubject(ctx: Context) {
        val subject = ctx.pathParam("subject")
        val removed = registry.deleteSubject(subject)
        if (removed.isEmpty()) {
            error(ctx, 404, 40401, "subject '$subject' not found")
            return
        }
        writeRaw(ctx, 200, json.writeValueAsBytes(removed))
    }

    // --- DELETE /subjects/{subject}/versions/{version} → version number ---
    @Throws(Exception::class)
    private fun deleteVersion(ctx: Context) {
        val subject = ctx.pathParam("subject")
        val version: Int
        try {
            version = Integer.parseInt(ctx.pathParam("version"))
        } catch (e: NumberFormatException) {
            error(ctx, 400, 40000, "version must be an integer")
            return
        }
        val deleted = registry.deleteVersion(subject, version)
        if (!deleted) {
            error(ctx, 404, 40402, "subject '$subject' version $version not found")
            return
        }
        writeRaw(ctx, 200, json.writeValueAsBytes(version))
    }

    // --- helpers ---
    @Throws(Exception::class)
    private fun write(ctx: Context, status: Int, body: Any?) {
        ctx.status(status)
            .contentType(CONTENT_TYPE)
            .result(json.writeValueAsBytes(body))
    }

    private fun writeRaw(ctx: Context, status: Int, body: ByteArray) {
        ctx.status(status).contentType(CONTENT_TYPE).result(body)
    }

    @Throws(Exception::class)
    private fun error(ctx: Context, status: Int, errorCode: Int, message: String?) {
        val body: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
        body.put("error_code", errorCode)
        body.put("message", message)
        write(ctx, status, body)
    }

    companion object {
        const val CONTENT_TYPE: String = "application/vnd.schemaregistry.v1+json"
    }
}
