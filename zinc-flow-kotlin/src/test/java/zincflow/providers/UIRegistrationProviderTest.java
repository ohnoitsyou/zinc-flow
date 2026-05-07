package zincflow.providers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/// Exercises the full heartbeat path against a real (but local)
/// HTTP server playing the UI's role.
final class UIRegistrationProviderTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private HttpServer fakeUi;
    private final List<Map<String, Object>> received = new CopyOnWriteArrayList<>();
    private CountDownLatch seen;

    @AfterEach
    void teardown() {
        if (fakeUi != null) fakeUi.stop(0);
    }

    private String startFakeUi(int expectedPosts) throws Exception {
        seen = new CountDownLatch(expectedPosts);
        fakeUi = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fakeUi.createContext("/register", exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = JSON.readValue(body, Map.class);
            received.add(payload);
            seen.countDown();
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().close();
        });
        fakeUi.start();
        return "http://127.0.0.1:" + fakeUi.getAddress().getPort() + "/register";
    }

    @Test
    void enableFiresImmediateHeartbeat() throws Exception {
        String url = startFakeUi(1);
        var provider = new UIRegistrationProvider(url,
                () -> Map.of("nodeId", "n1", "hostname", "h1", "port", 9092));
        provider.enable();

        assertTrue(seen.await(2, TimeUnit.SECONDS), "expected a heartbeat within 2s");
        assertEquals(1, received.size());
        assertEquals("n1", received.get(0).get("nodeId"));
        // lastStatus is set after the client-side send() returns; the
        // server's countDown fires first, so poll briefly for the
        // post-response update to land.
        for (int i = 0; i < 20 && !Integer.valueOf(200).equals(provider.lastOutcome().get("lastStatus")); i++) {
            Thread.sleep(50);
        }
        assertEquals(200, provider.lastOutcome().get("lastStatus"));

        provider.shutdown();
    }

    @Test
    void repeatedHeartbeatsFire() throws Exception {
        String url = startFakeUi(2);
        var provider = new UIRegistrationProvider(url,
                () -> Map.of("nodeId", "n2"),
                1,
                java.net.http.HttpClient.newHttpClient());
        provider.enable();
        try {
            assertTrue(seen.await(4, TimeUnit.SECONDS),
                    "expected at least 2 heartbeats in 4s");
            assertTrue(received.size() >= 2);
        } finally {
            provider.shutdown();
        }
    }

    @Test
    void disableStopsHeartbeat() throws Exception {
        String url = startFakeUi(1);
        var provider = new UIRegistrationProvider(url,
                () -> Map.of("nodeId", "n3"),
                1,
                java.net.http.HttpClient.newHttpClient());
        provider.enable();
        assertTrue(seen.await(2, TimeUnit.SECONDS));
        provider.disable(0);

        int before = received.size();
        Thread.sleep(1500);
        assertEquals(before, received.size(),
                "no heartbeats should arrive after disable");
    }

    @Test
    void rejectsBlankTarget() {
        assertThrows(IllegalArgumentException.class,
                () -> new UIRegistrationProvider("", Map::of));
    }
}
