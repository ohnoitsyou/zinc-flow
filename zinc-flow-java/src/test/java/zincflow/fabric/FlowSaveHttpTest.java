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
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

final class FlowSaveHttpTest {

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

    private HttpResponse<String> post(String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base() + path))
                        .header("content-type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void flowSaveWritesYamlToBasePath(@TempDir Path dir) throws Exception {
        Path base = dir.resolve("config.yaml");
        Files.writeString(base, """
                flow:
                  entryPoints: [ingress]
                  processors:
                    ingress:
                      type: LogAttribute
                      config:
                        prefix: "[hello] "
                """);
        server = boot(base);

        var resp = post("/api/flow/save", "");
        assertEquals(200, resp.statusCode(), resp.body());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals("saved", body.get("status").asText());
        assertEquals(base.toString(), body.get("path").asText());

        // File on disk parses back to the same graph shape.
        String written = Files.readString(base);
        var reloaded = new ConfigLoader(new Registry()).load(written);
        assertTrue(reloaded.processors().containsKey("ingress"));
        assertEquals(java.util.List.of("ingress"), reloaded.entryPoints());
    }

    @Test
    void flowSaveWithoutLoaderReturns501() throws Exception {
        var pipeline = new Pipeline(PipelineGraph.empty());
        server = new HttpServer(pipeline).start(0);
        var resp = post("/api/flow/save", "");
        assertEquals(501, resp.statusCode());
    }
}
