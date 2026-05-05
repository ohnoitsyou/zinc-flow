package zincflow.fabric;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zincflow.core.ProcessorContext;
import zincflow.providers.ConfigProvider;
import zincflow.providers.LoggingProvider;
import zincflow.providers.ProvenanceProvider;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/// End-to-end tests for the Phase 3e management endpoints: providers,
/// provenance, processor admin, sources, registry. Each test boots the
/// Javalin server on an ephemeral port and hits it through the standard
/// JDK HttpClient — this keeps the surface we test exactly the one a
/// dashboard or control plane would actually consume.
final class HttpServerAdminTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private HttpServer server;
    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
    }

    private String base() {
        return "http://localhost:" + server.port();
    }

    private HttpServer boot() {
        var context = new ProcessorContext();
        var logging = new LoggingProvider();
        logging.enable();
        context.addProvider(logging);
        var cfg = new ConfigProvider(Map.of());
        cfg.enable();
        context.addProvider(cfg);
        var prov = new ProvenanceProvider();
        prov.enable();
        context.addProvider(prov);

        var registry = new Registry();
        var pipeline = new Pipeline(PipelineGraph.empty(), Pipeline.DEFAULT_MAX_HOPS, null, context, registry);
        server = new HttpServer(pipeline).start(0);
        return server;
    }

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

    private HttpResponse<String> deleteJson(String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base() + path))
                        .header("content-type", "application/json")
                        .method("DELETE", HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    @SuppressWarnings("unchecked")
    void providersListsWiredProviders() throws Exception {
        boot();
        var resp = get("/api/providers");
        assertEquals(200, resp.statusCode());
        List<Map<String, Object>> list = JSON.readValue(resp.body(), List.class);
        assertTrue(list.stream().anyMatch(m -> "logging".equals(m.get("name"))));
        assertTrue(list.stream().anyMatch(m -> "config".equals(m.get("name"))));
        assertTrue(list.stream().anyMatch(m -> "provenance".equals(m.get("name"))));
    }

    @Test
    void enableAndDisableProvider() throws Exception {
        boot();
        var disable = postJson("/api/providers/disable", "{\"name\":\"logging\"}");
        assertEquals(200, disable.statusCode());
        assertTrue(disable.body().contains("disabled"));

        var enable = postJson("/api/providers/enable", "{\"name\":\"logging\"}");
        assertEquals(200, enable.statusCode());
        assertTrue(enable.body().contains("enabled"));

        var missing = postJson("/api/providers/enable", "{\"name\":\"nosuch\"}");
        assertEquals(404, missing.statusCode());
    }

    @Test
    void registryListsBuiltinTypes() throws Exception {
        boot();
        var resp = get("/api/registry");
        assertEquals(200, resp.statusCode());
        assertTrue(resp.body().contains("LogAttribute"));
        assertTrue(resp.body().contains("ExtractText"));
        assertTrue(resp.body().contains("RouteOnAttribute"));
    }

    @Test
    void processorAddEnableDisableRemoveStateLifecycle() throws Exception {
        boot();

        var add = postJson("/api/processors/add",
                "{\"name\":\"logger\",\"type\":\"LogAttribute\",\"config\":{\"prefix\":\"[x] \"}}");
        assertEquals(200, add.statusCode());
        assertTrue(add.body().contains("created"));

        var state = postJson("/api/processors/state", "{\"name\":\"logger\"}");
        assertEquals(200, state.statusCode());
        assertTrue(state.body().contains("ENABLED"));

        assertEquals(200, postJson("/api/processors/disable", "{\"name\":\"logger\"}").statusCode());
        assertTrue(postJson("/api/processors/state", "{\"name\":\"logger\"}").body().contains("DISABLED"));

        assertEquals(200, postJson("/api/processors/enable", "{\"name\":\"logger\"}").statusCode());
        assertEquals(200, deleteJson("/api/processors/remove", "{\"name\":\"logger\"}").statusCode());

        // Post-removal, state lookup 404s
        var after = postJson("/api/processors/state", "{\"name\":\"logger\"}");
        assertEquals(404, after.statusCode());
    }

    @Test
    void processorStatsReturnsPerProcessorCounters() throws Exception {
        boot();
        postJson("/api/processors/add",
                "{\"name\":\"logger\",\"type\":\"LogAttribute\",\"config\":{}}");

        var resp = get("/api/processor-stats");
        assertEquals(200, resp.statusCode());
        assertTrue(resp.body().contains("logger"));
        assertTrue(resp.body().contains("processed"));
    }

    @Test
    void provenanceReturns503WhenProviderMissing() throws Exception {
        // Boot a server whose context has no provenance provider — we need to
        // build a bespoke one rather than reuse boot().
        var context = new ProcessorContext();
        var pipeline = new Pipeline(PipelineGraph.empty(), Pipeline.DEFAULT_MAX_HOPS, null, context, new Registry());
        server = new HttpServer(pipeline).start(0);
        var resp = get("/api/provenance");
        assertEquals(503, resp.statusCode());
        assertTrue(resp.body().contains("provenance provider not enabled"));
    }

    @Test
    void provenanceReturnsEmptyListWhenNoEventsRecorded() throws Exception {
        boot();
        var resp = get("/api/provenance");
        assertEquals(200, resp.statusCode());
        assertEquals("[]", resp.body().trim());
    }

    @Test
    void sourcesEndpointReturnsEmptyListByDefault() throws Exception {
        boot();
        var resp = get("/api/sources");
        assertEquals(200, resp.statusCode());
        assertEquals("[]", resp.body().trim());
    }

    @Test
    void emptyNameRejected() throws Exception {
        boot();
        var resp = postJson("/api/processors/enable", "{}");
        assertEquals(400, resp.statusCode());
        assertTrue(resp.body().contains("name required"));
    }
}
