package zincflow.fabric;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import zincflow.core.ProcessorContext;
import zincflow.providers.VersionControlProvider;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

final class VcHttpTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private HttpServer server;
    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void teardown() { if (server != null) server.stop(); }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(
                URI.create("http://localhost:" + server.port() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(
                URI.create("http://localhost:" + server.port() + path))
                        .header("content-type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void statusReturnsDisabledWhenProviderMissing() throws Exception {
        var pipeline = new Pipeline(PipelineGraph.empty());
        server = new HttpServer(pipeline).start(0);
        var resp = get("/api/vc/status");
        assertEquals(200, resp.statusCode());
        assertEquals(false, JSON.readTree(resp.body()).get("enabled").asBoolean());
    }

    @Test
    void commitRejectedWhenProviderNotEnabled() throws Exception {
        var pipeline = new Pipeline(PipelineGraph.empty());
        server = new HttpServer(pipeline).start(0);
        var resp = post("/api/vc/commit", "{\"message\":\"hi\"}");
        assertEquals(503, resp.statusCode());
    }

    @Test
    void commitBlankMessageReturns400(@TempDir Path dir) throws Exception {
        initRepo(dir);
        var ctx = new ProcessorContext();
        var vc = new VersionControlProvider(dir, null, null, "main");
        vc.enable();
        ctx.addProvider(vc);
        var pipeline = new Pipeline(PipelineGraph.empty(), Pipeline.DEFAULT_MAX_HOPS, null, ctx, new Registry());
        server = new HttpServer(pipeline).start(0);
        var resp = post("/api/vc/commit", "{}");
        assertEquals(400, resp.statusCode());
    }

    @Test
    void statusAndCommitHappyPath(@TempDir Path dir) throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(gitAvailable(), "git not on PATH");
        initRepo(dir);
        Files.writeString(dir.resolve("note.txt"), "content");

        var ctx = new ProcessorContext();
        var vc = new VersionControlProvider(dir, null, null, "main");
        vc.enable();
        ctx.addProvider(vc);
        var pipeline = new Pipeline(PipelineGraph.empty(), Pipeline.DEFAULT_MAX_HOPS, null, ctx, new Registry());
        server = new HttpServer(pipeline).start(0);

        var statusResp = get("/api/vc/status");
        assertEquals(200, statusResp.statusCode());
        JsonNode status = JSON.readTree(statusResp.body());
        assertTrue(status.get("enabled").asBoolean());

        var commitResp = post("/api/vc/commit",
                "{\"path\":\"note.txt\",\"message\":\"add note\"}");
        assertEquals(200, commitResp.statusCode(), commitResp.body());
        assertTrue(JSON.readTree(commitResp.body()).get("ok").asBoolean());
    }

    private static boolean gitAvailable() {
        try { return new ProcessBuilder("git", "--version").start().waitFor() == 0; }
        catch (Exception e) { return false; }
    }

    private static void initRepo(Path dir) throws Exception {
        run(dir, "git", "init", "-b", "main");
        run(dir, "git", "config", "user.email", "test@example.com");
        run(dir, "git", "config", "user.name", "Zinc Test");
        Files.writeString(dir.resolve("README.md"), "hi\n");
        run(dir, "git", "add", "README.md");
        run(dir, "git", "commit", "-m", "init");
    }

    private static void run(Path dir, String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd).directory(dir.toFile()).redirectErrorStream(true).start();
        if (p.waitFor() != 0) {
            throw new AssertionError("command " + List.of(cmd) + " failed: "
                    + new String(p.getInputStream().readAllBytes()));
        }
    }
}
