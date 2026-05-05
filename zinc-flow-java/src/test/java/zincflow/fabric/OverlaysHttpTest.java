package zincflow.fabric;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

final class OverlaysHttpTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private HttpServer server;
    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void teardown() { if (server != null) server.stop(); }

    private HttpServer boot(Path base) throws Exception {
        var registry = new Registry();
        var loader = new ConfigLoader(registry);
        var graph = loader.loadFromFile(base);
        var pipeline = new Pipeline(graph, Pipeline.DEFAULT_MAX_HOPS, null, loader.context(), registry);
        return new HttpServer(pipeline, loader, base, null, null).start(0);
    }

    private String base() { return "http://localhost:" + server.port(); }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> put(String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base() + path))
                        .header("content-type", "application/json")
                        .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void overlaysEndpointReportsLayerStack(@TempDir Path dir) throws Exception {
        Path baseYaml = dir.resolve("config.yaml");
        Files.writeString(baseYaml, """
                flow:
                  entryPoints: [a]
                  processors:
                    a:
                      type: LogAttribute
                      config:
                        prefix: "[base] "
                """);
        Path local = dir.resolve("config.local.yaml");
        Files.writeString(local, """
                flow:
                  processors:
                    a:
                      config:
                        prefix: "[local] "
                """);
        server = boot(baseYaml);

        var resp = get("/api/overlays");
        assertEquals(200, resp.statusCode(), resp.body());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals(baseYaml.toString(), body.get("base").asText());
        assertEquals(3, body.get("layers").size());
        // Local overlay overrode the base prefix.
        assertEquals("local",
                body.get("provenance").get("flow.processors.a.config.prefix").asText());
    }

    @Test
    void overlaysWithoutConfigLoaderReturns503() throws Exception {
        // Boot without a loader — the GET should surface 503 rather than a NPE.
        var pipeline = new Pipeline(PipelineGraph.empty());
        server = new HttpServer(pipeline).start(0);
        var resp = get("/api/overlays");
        assertEquals(503, resp.statusCode());
    }

    @Test
    void writeSecretsEndpointIsRetired(@TempDir Path dir) throws Exception {
        Path baseYaml = dir.resolve("config.yaml");
        Files.writeString(baseYaml, "flow:\n  entryPoints: [a]\n  processors:\n    a: {type: LogAttribute}\n");
        server = boot(baseYaml);
        var resp = put("/api/overlays/secrets",
                "{\"flow\":{\"processors\":{\"a\":{\"config\":{\"token\":\"T\"}}}}}");
        assertEquals(404, resp.statusCode());
        assertFalse(Files.exists(dir.resolve("secrets.yaml")),
                "retired endpoint must not write secrets.yaml to disk");
    }
}
