package zincflow.fabric;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

final class IdentityHttpTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private HttpServer server;
    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void teardown() { if (server != null) server.stop(); }

    @Test
    void identityEndpointReportsNodeIdAndPort() throws Exception {
        var identity = new NodeIdentity("node-42", "my-host", "9.0.0");
        var pipeline = new Pipeline(PipelineGraph.empty());
        server = new HttpServer(pipeline, null, null, null, null, identity).start(0);

        var resp = http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + "/api/identity"))
                        .GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals("node-42", body.get("nodeId").asText());
        assertEquals("my-host", body.get("hostname").asText());
        assertEquals("9.0.0", body.get("version").asText());
        assertEquals(server.port(), body.get("port").asInt());
        assertTrue(body.get("uptimeMillis").asLong() >= 0);
    }

    @Test
    void identityEndpointReturns503WhenNotWired() throws Exception {
        var pipeline = new Pipeline(PipelineGraph.empty());
        server = new HttpServer(pipeline).start(0);
        var resp = http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + "/api/identity"))
                        .GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(503, resp.statusCode());
    }
}
