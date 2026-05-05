package zincflow.fabric;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Proves the Jetty virtual-thread executor actually serves ingests
/// concurrently. The entry processor sleeps {@value #SLEEP_MS} ms per
/// FlowFile; if requests were serialized on a platform pool, the wall
/// time for {@value #CONCURRENCY} requests would approach
/// {@code CONCURRENCY * SLEEP_MS}. With virtual threads it stays
/// close to one sleep interval plus overhead.
final class VirtualThreadIngestTest {

    private static final int CONCURRENCY = 200;
    private static final int SLEEP_MS = 100;

    private HttpServer server;

    @AfterEach
    void teardown() { if (server != null) server.stop(); }

    @Test
    void concurrentIngestsRunInParallel() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        Processor sleeper = new Processor() {
            @Override public String name() { return "sleeper"; }
            @Override public ProcessorResult process(FlowFile ff) {
                invocations.incrementAndGet();
                try { Thread.sleep(SLEEP_MS); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                return ProcessorResult.Dropped.INSTANCE;
            }
        };

        var graph = new PipelineGraph(Map.of("sleeper", sleeper), Map.of(), List.of("sleeper"));
        var pipeline = new Pipeline(graph);
        server = new HttpServer(pipeline).start(0);

        URI target = URI.create("http://localhost:" + server.port() + "/");
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        try (ExecutorService launcher = Executors.newVirtualThreadPerTaskExecutor()) {
            long start = System.nanoTime();
            List<CompletableFuture<HttpResponse<String>>> futures = new java.util.ArrayList<>(CONCURRENCY);
            for (int i = 0; i < CONCURRENCY; i++) {
                futures.add(CompletableFuture.supplyAsync(() -> {
                    try {
                        return http.send(HttpRequest.newBuilder(target)
                                        .timeout(Duration.ofSeconds(30))
                                        .POST(HttpRequest.BodyPublishers.ofString("ping"))
                                        .build(),
                                HttpResponse.BodyHandlers.ofString());
                    } catch (Exception ex) { throw new RuntimeException(ex); }
                }, launcher));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            for (var f : futures) {
                assertEquals(202, f.get().statusCode(), "every ingest must succeed");
            }
            assertEquals(CONCURRENCY, invocations.get());

            // Serialised ceiling: CONCURRENCY * SLEEP_MS = 20s. A working
            // virtual-thread pool finishes in well under 5s on any
            // reasonable CI box. Budget 10s to stay comfortable.
            assertTrue(elapsedMs < 10_000,
                    () -> "ingests appear serialised (took " + elapsedMs + "ms for "
                            + CONCURRENCY + " × " + SLEEP_MS + "ms sleeps)");
        }
    }
}
