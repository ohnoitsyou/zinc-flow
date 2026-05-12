package zincflow.fabric;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zincflow.core.ProcessorContext;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/// End-to-end HTTP tests for the Phase 3f graph mutation endpoints:
/// POST/DELETE /api/connections, PUT /api/connections/{from},
/// PUT /api/entrypoints.
final class GraphMutationHttpTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private HttpServer server;
    private Pipeline pipeline;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void boot() {
        var registry = new ProcessorRegistry();
        var context = new ProcessorContext();
        pipeline = new Pipeline(PipelineGraph.Companion.empty(), Pipeline.DEFAULT_MAX_HOPS, null, context, registry);
        // Seed three processors via the admin API semantics so they're
        // routable once edges land.
        pipeline.addProcessor("a", "LogAttribute", java.util.Map.of(), java.util.List.of(), java.util.Map.of());
        pipeline.addProcessor("b", "LogAttribute", java.util.Map.of(), java.util.List.of(), java.util.Map.of());
        pipeline.addProcessor("c", "LogAttribute", java.util.Map.of(), java.util.List.of(), java.util.Map.of());
        // Entry point is 'a' by default-of-convention.
        pipeline.setEntryPoints(java.util.List.of("a"));
        server = new HttpServer(pipeline).start(0);
    }

    @AfterEach
    void tearDown() { if (server != null) server.stop(); }

    private String base() { return "http://localhost:" + server.port(); }

    private HttpResponse<String> send(String method, String path, String body) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(base() + path))
                .header("content-type", "application/json");
        HttpRequest req = switch (method) {
            case "POST"   -> builder.POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build();
            case "PUT"    -> builder.PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build();
            case "DELETE" -> builder.method("DELETE", HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build();
            default -> throw new IllegalArgumentException("unsupported: " + method);
        };
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void addConnectionOk() throws Exception {
        var resp = send("POST", "/api/connections",
                "{\"from\":\"a\",\"relationship\":\"success\",\"to\":\"b\"}");
        assertEquals(200, resp.statusCode(), resp.body());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals("added", body.get("status").asText());
        assertEquals(java.util.List.of("b"), pipeline.graph().next("a", "success"));
    }

    @Test
    void addConnectionDuplicate409() throws Exception {
        assertTrue(pipeline.addConnection("a", "success", "b").ok());
        var resp = send("POST", "/api/connections",
                "{\"from\":\"a\",\"relationship\":\"success\",\"to\":\"b\"}");
        assertEquals(409, resp.statusCode());
        assertTrue(JSON.readTree(resp.body()).get("error").asText().contains("already exists"));
    }

    @Test
    void addConnectionUnknownProcessor409() throws Exception {
        var resp = send("POST", "/api/connections",
                "{\"from\":\"ghost\",\"relationship\":\"success\",\"to\":\"b\"}");
        assertEquals(409, resp.statusCode());
    }

    @Test
    void removeConnectionOk() throws Exception {
        assertTrue(pipeline.addConnection("a", "success", "b").ok());
        var resp = send("DELETE", "/api/connections",
                "{\"from\":\"a\",\"relationship\":\"success\",\"to\":\"b\"}");
        assertEquals(200, resp.statusCode());
        assertEquals(java.util.List.of(), pipeline.graph().next("a", "success"));
    }

    @Test
    void removeConnectionMissing409() throws Exception {
        var resp = send("DELETE", "/api/connections",
                "{\"from\":\"a\",\"relationship\":\"success\",\"to\":\"b\"}");
        assertEquals(409, resp.statusCode());
    }

    @Test
    void setConnectionsReplacesOutbound() throws Exception {
        var resp = send("PUT", "/api/connections/a",
                "{\"high\":[\"b\"],\"low\":[\"c\"]}");
        assertEquals(200, resp.statusCode(), resp.body());
        assertEquals(java.util.List.of("b"), pipeline.graph().next("a", "high"));
        assertEquals(java.util.List.of("c"), pipeline.graph().next("a", "low"));
    }

    @Test
    void setConnectionsValidatesTargets() throws Exception {
        var resp = send("PUT", "/api/connections/a",
                "{\"bad\":[\"ghost\"]}");
        assertEquals(409, resp.statusCode());
    }

    @Test
    void setConnectionsMalformedBody400() throws Exception {
        // Relationship should map to a list, but we send a string.
        var resp = send("PUT", "/api/connections/a", "{\"success\":\"b\"}");
        assertEquals(400, resp.statusCode());
    }

    @Test
    void setEntryPointsReplacesSet() throws Exception {
        var resp = send("PUT", "/api/entrypoints",
                "{\"names\":[\"b\",\"c\"]}");
        assertEquals(200, resp.statusCode());
        assertEquals(java.util.List.of("b", "c"), pipeline.graph().getEntryPoints());
    }

    @Test
    void setEntryPointsUnknownName409() throws Exception {
        var resp = send("PUT", "/api/entrypoints",
                "{\"names\":[\"ghost\"]}");
        assertEquals(409, resp.statusCode());
    }

    @Test
    void setEntryPointsMissingNamesField400() throws Exception {
        var resp = send("PUT", "/api/entrypoints", "{}");
        assertEquals(400, resp.statusCode());
    }
}
