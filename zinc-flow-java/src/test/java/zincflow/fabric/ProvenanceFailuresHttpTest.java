package zincflow.fabric;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zincflow.core.ProcessorContext;
import zincflow.providers.ProvenanceProvider;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

final class ProvenanceFailuresHttpTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private HttpServer server;
    private ProvenanceProvider prov;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void boot() {
        prov = new ProvenanceProvider();
        prov.enable();
        var context = new ProcessorContext();
        context.addProvider(prov);
        var pipeline = new Pipeline(PipelineGraph.empty(), Pipeline.DEFAULT_MAX_HOPS, null, context, new Registry());
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
    void failuresEndpointFiltersByEventType() throws Exception {
        prov.record(1, ProvenanceProvider.EventType.PROCESSED, "a");
        prov.record(2, ProvenanceProvider.EventType.FAILED, "b", "boom");
        prov.record(3, ProvenanceProvider.EventType.PROCESSED, "c");
        prov.record(4, ProvenanceProvider.EventType.FAILED, "d", "splat");

        var resp = get("/api/provenance/failures?n=10");
        assertEquals(200, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals(2, body.size());
        // Most recent failures come first since the provider returns
        // oldest→newest within the window and we collect forward.
        assertEquals("FAILED", body.get(0).get("type").asText());
        assertEquals("FAILED", body.get(1).get("type").asText());
    }

    @Test
    void failuresEndpointHonorsN() throws Exception {
        for (int i = 0; i < 10; i++) {
            prov.record(i, ProvenanceProvider.EventType.FAILED, "p", "fail");
        }
        var resp = get("/api/provenance/failures?n=3");
        assertEquals(200, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals(3, body.size());
    }

    @Test
    void failuresBadNParamReturns400() throws Exception {
        var resp = get("/api/provenance/failures?n=abc");
        assertEquals(400, resp.statusCode());
    }

    @Test
    void lineageEndpointReturnsEventsForFlowFileId() throws Exception {
        prov.record(42, ProvenanceProvider.EventType.CREATED, "ingress");
        prov.record(42, ProvenanceProvider.EventType.PROCESSED, "stage");
        prov.record(99, ProvenanceProvider.EventType.PROCESSED, "other");

        var resp = get("/api/provenance/42");
        assertEquals(200, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals(2, body.size());
    }

    @Test
    void lineageWithoutProviderReturns503() throws Exception {
        server.stop();
        var pipeline = new Pipeline(PipelineGraph.empty());
        server = new HttpServer(pipeline).start(0);
        var resp = get("/api/provenance/1");
        assertEquals(503, resp.statusCode());
    }
}
