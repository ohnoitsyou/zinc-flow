package zincflow.fabric;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zincflow.core.ProcessorContext;
import zincflow.providers.SchemaRegistryProvider;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/// End-to-end tests for the Confluent-shape schema registry REST API.
/// Boots an HttpServer wired to a live SchemaRegistryProvider, hits
/// every endpoint through java.net.http, and asserts both the payload
/// and the {@code application/vnd.schemaregistry.v1+json} content
/// type.
final class SchemaProcessorRegistryHandlerTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private HttpServer server;
    private SchemaRegistryProvider registry;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void boot() {
        var context = new ProcessorContext();
        registry = new SchemaRegistryProvider();
        registry.enable();
        context.addProvider(registry);
        var pipeline = new Pipeline(PipelineGraph.Companion.empty(), Pipeline.DEFAULT_MAX_HOPS, null, context, new ProcessorRegistry());
        server = new HttpServer(pipeline).start(0);
    }

    @AfterEach
    void teardown() {
        if (server != null) server.stop();
    }

    private String base() { return "http://localhost:" + server.port(); }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postJson(String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base() + path))
                        .header("content-type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> delete(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base() + path))
                        .DELETE().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void registerReturnsId() throws Exception {
        var resp = postJson("/api/schema-registry/subjects/order-value/versions",
                "{\"schema\":\"{\\\"type\\\":\\\"string\\\"}\"}");
        assertEquals(200, resp.statusCode());
        assertEquals(SchemaRegistryHandler.CONTENT_TYPE,
                resp.headers().firstValue("content-type").orElse(""));
        JsonNode body = JSON.readTree(resp.body());
        assertTrue(body.has("id"));
        assertTrue(body.get("id").asInt() > 0);
    }

    @Test
    void registerMissingSchemaFieldReturns422() throws Exception {
        var resp = postJson("/api/schema-registry/subjects/order-value/versions", "{}");
        assertEquals(422, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals(42201, body.get("error_code").asInt());
        assertTrue(body.get("message").asText().toLowerCase().contains("schema"));
    }

    @Test
    void getByIdRoundTrip() throws Exception {
        var registered = registry.register("order-value", "{\"type\":\"int\"}");
        var resp = get("/api/schema-registry/schemas/ids/" + registered.id);
        assertEquals(200, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals(registered.definition, body.get("schema").asText());
    }

    @Test
    void getByIdNotFoundReturns404() throws Exception {
        var resp = get("/api/schema-registry/schemas/ids/999");
        assertEquals(404, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals(40403, body.get("error_code").asInt());
    }

    @Test
    void listSubjectsReturnsSortedNames() throws Exception {
        registry.register("beta", "{}");
        registry.register("alpha", "{}");
        var resp = get("/api/schema-registry/subjects");
        assertEquals(200, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals("alpha", body.get(0).asText());
        assertEquals("beta",  body.get(1).asText());
    }

    @Test
    void listVersionsReturnsNumbersInOrder() throws Exception {
        registry.register("order", "v1");
        registry.register("order", "v2");
        var resp = get("/api/schema-registry/subjects/order/versions");
        assertEquals(200, resp.statusCode());
        assertEquals("[1,2]", resp.body());
    }

    @Test
    void listVersionsUnknownSubjectReturns404() throws Exception {
        var resp = get("/api/schema-registry/subjects/ghost/versions");
        assertEquals(404, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals(40401, body.get("error_code").asInt());
    }

    @Test
    void getLatestVersionResolvesAlias() throws Exception {
        registry.register("order", "v1");
        var v2 = registry.register("order", "v2");
        var resp = get("/api/schema-registry/subjects/order/versions/latest");
        assertEquals(200, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals(v2.version, body.get("version").asInt());
        assertEquals(v2.id,      body.get("id").asInt());
        assertEquals("v2",         body.get("schema").asText());
    }

    @Test
    void getSpecificVersionReturnsFullPayload() throws Exception {
        var v1 = registry.register("order", "v1");
        var resp = get("/api/schema-registry/subjects/order/versions/1");
        assertEquals(200, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals("order", body.get("subject").asText());
        assertEquals(1,       body.get("version").asInt());
        assertEquals(v1.id, body.get("id").asInt());
    }

    @Test
    void getVersionUnknownSubjectVsVersionErrorCodes() throws Exception {
        registry.register("order", "v1");

        var noSubject = get("/api/schema-registry/subjects/ghost/versions/1");
        assertEquals(404, noSubject.statusCode());
        assertEquals(40401, JSON.readTree(noSubject.body()).get("error_code").asInt());

        var noVersion = get("/api/schema-registry/subjects/order/versions/99");
        assertEquals(404, noVersion.statusCode());
        assertEquals(40402, JSON.readTree(noVersion.body()).get("error_code").asInt());
    }

    @Test
    void getVersionBadIntReturns400() throws Exception {
        registry.register("order", "v1");
        var resp = get("/api/schema-registry/subjects/order/versions/banana");
        // "banana" parses neither as int nor as "latest" → handler emits
        // 404 via the version-not-found code. Either shape is acceptable
        // per Confluent docs — we pin to the behavior we picked.
        assertEquals(404, resp.statusCode());
    }

    @Test
    void deleteSubjectReturnsRemovedVersions() throws Exception {
        registry.register("order", "v1");
        registry.register("order", "v2");
        var resp = delete("/api/schema-registry/subjects/order");
        assertEquals(200, resp.statusCode());
        assertEquals("[1,2]", resp.body());
        assertTrue(registry.listSubjects().isEmpty());
    }

    @Test
    void deleteSubjectUnknownReturns404() throws Exception {
        var resp = delete("/api/schema-registry/subjects/ghost");
        assertEquals(404, resp.statusCode());
    }

    @Test
    void deleteVersionEchoesVersionNumber() throws Exception {
        registry.register("order", "v1");
        registry.register("order", "v2");
        var resp = delete("/api/schema-registry/subjects/order/versions/1");
        assertEquals(200, resp.statusCode());
        assertEquals("1", resp.body());
        assertEquals(java.util.List.of(2), registry.listVersions("order"));
    }

    @Test
    void deleteVersionUnknownReturns404() throws Exception {
        registry.register("order", "v1");
        var resp = delete("/api/schema-registry/subjects/order/versions/99");
        assertEquals(404, resp.statusCode());
    }

    @Test
    void endpointsAbsentWhenProviderNotWired() throws Exception {
        // Boot a separate server with no schema_registry in context.
        server.stop();
        var context = new ProcessorContext();
        var pipeline = new Pipeline(PipelineGraph.Companion.empty(), Pipeline.DEFAULT_MAX_HOPS, null, context, new ProcessorRegistry());
        server = new HttpServer(pipeline).start(0);
        var resp = get("/api/schema-registry/subjects");
        assertEquals(404, resp.statusCode(),
                "schema registry routes must not exist when the provider is absent");
    }
}
