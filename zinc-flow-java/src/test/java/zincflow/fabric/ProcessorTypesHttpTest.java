package zincflow.fabric;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zincflow.core.ProcessorContext;
import zincflow.core.ProcessorResult;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

final class ProcessorTypesHttpTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private HttpServer server;
    private Registry registry;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void boot() {
        registry = new Registry();
        registry.register(new Registry.TypeInfo(
                "Toy", "1.0.0", "A sample processor",
                java.util.List.of("key", "value"),
                java.util.List.of("success", "failure")),
                (cfg, ctx) -> ff -> ProcessorResult.Dropped );
        registry.register(new Registry.TypeInfo(
                "Toy", "2.0.0", "Revised sample processor",
                java.util.List.of("key", "value", "mode"),
                java.util.List.of("success", "failure", "skipped")),
                (cfg, ctx) -> ff -> ProcessorResult.dropped());

        var pipeline = new Pipeline(PipelineGraph.empty(), Pipeline.DEFAULT_MAX_HOPS, null,
                new ProcessorContext(), registry);
        server = new HttpServer(pipeline).start(0);
    }

    @AfterEach
    void teardown() { if (server != null) server.stop(); }

    private String base() { return "http://localhost:" + server.port(); }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void listProcessorTypes() throws Exception {
        var resp = get("/api/processor-types");
        assertEquals(200, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertTrue(body.isArray());
        boolean foundToyV1 = false, foundToyV2 = false;
        for (JsonNode entry : body) {
            if ("Toy".equals(entry.get("name").asText())) {
                if ("1.0.0".equals(entry.get("version").asText())) foundToyV1 = true;
                if ("2.0.0".equals(entry.get("version").asText())) foundToyV2 = true;
            }
        }
        assertTrue(foundToyV1, "expected Toy@1.0.0 in listing");
        assertTrue(foundToyV2, "expected Toy@2.0.0 in listing");
    }

    @Test
    void listVersionsForSingleType() throws Exception {
        var resp = get("/api/processor-types/Toy");
        assertEquals(200, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals("Toy", body.get("name").asText());
        assertEquals("2.0.0", body.get("latest").asText());
        assertEquals(2, body.get("versions").size());

        // Latest's metadata surfaces (configKeys, relationships).
        JsonNode v2 = null;
        for (JsonNode v : body.get("versions")) {
            if ("2.0.0".equals(v.get("version").asText())) { v2 = v; break; }
        }
        assertNotNull(v2);
        assertEquals(3, v2.get("configKeys").size());
        assertEquals("skipped", v2.get("relationships").get(2).asText());
    }

    @Test
    void unknownTypeReturns404() throws Exception {
        var resp = get("/api/processor-types/Ghost");
        assertEquals(404, resp.statusCode());
    }
}
