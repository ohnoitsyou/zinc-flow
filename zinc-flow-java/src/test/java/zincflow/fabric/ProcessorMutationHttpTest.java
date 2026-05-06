package zincflow.fabric;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zincflow.core.ProcessorContext;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

/// PUT /api/processors/{name}/config + /connections coverage.
final class ProcessorMutationHttpTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private HttpServer server;
    private Pipeline pipeline;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void boot() {
        var registry = new Registry();
        var context = new ProcessorContext();
        pipeline = new Pipeline(PipelineGraph.Companion.empty(), Pipeline.DEFAULT_MAX_HOPS, null, context, registry);
        pipeline.addProcessor("router", "RouteOnAttribute",
                java.util.Map.of("routes", "high: priority == urgent"),
                java.util.List.of(), java.util.Map.of());
        pipeline.addProcessor("logger", "LogAttribute",
                java.util.Map.of(), java.util.List.of(), java.util.Map.of());
        server = new HttpServer(pipeline).start(0);
    }

    @AfterEach
    void teardown() { if (server != null) server.stop(); }

    private String base() { return "http://localhost:" + server.port(); }

    private HttpResponse<String> put(String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base() + path))
                        .header("content-type", "application/json")
                        .PUT(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void updateProcessorConfigRebuildsInstance() throws Exception {
        var before = pipeline.graph().getProcessors().get("router");

        var resp = put("/api/processors/router/config",
                "{\"config\":{\"routes\":\"low: priority == normal\"}}");
        assertEquals(200, resp.statusCode(), resp.body());

        var after = pipeline.graph().getProcessors().get("router");
        assertNotSame(before, after, "config update must rebuild the instance");
    }

    @Test
    void updateProcessorConfigUnknownName409() throws Exception {
        var resp = put("/api/processors/ghost/config", "{\"config\":{}}");
        assertEquals(409, resp.statusCode());
    }

    @Test
    void updateProcessorConfigRejectsUnknownType() throws Exception {
        var resp = put("/api/processors/router/config",
                "{\"type\":\"Nonexistent\",\"config\":{}}");
        assertEquals(409, resp.statusCode());
        assertTrue(JSON.readTree(resp.body()).get("error").asText().toLowerCase().contains("unknown"));
    }

    @Test
    void setConnectionsAliasReplacesOutbound() throws Exception {
        var resp = put("/api/processors/router/connections",
                "{\"high\":[\"logger\"]}");
        assertEquals(200, resp.statusCode(), resp.body());
        assertEquals(java.util.List.of("logger"), pipeline.graph().next("router", "high"));
    }

    @Test
    void setConnectionsAliasRejectsUnknownTarget() throws Exception {
        var resp = put("/api/processors/router/connections",
                "{\"high\":[\"ghost\"]}");
        assertEquals(409, resp.statusCode());
    }

    @Test
    void setConnectionsAliasRejectsMalformedBody() throws Exception {
        var resp = put("/api/processors/router/connections",
                "{\"high\":\"logger\"}");
        assertEquals(400, resp.statusCode());
    }
}
