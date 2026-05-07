package zincflow.sources;

import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;
import zincflow.core.PollingSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

final class PollingSourceTest {

    @Test
    void pollLoopEmitsAndCallsOnIngested() throws Exception {
        CountDownLatch ingestedOnce = new CountDownLatch(1);
        AtomicInteger pollCount = new AtomicInteger();
        List<Long> ingestedIds = new CopyOnWriteArrayList<>();

        PollingSource source = new PollingSource("probe", 10) {
            @Override public boolean isRunning() { return getRunning(); }
            @Override public String sourceType() { return "probe"; }
            @Override protected List<FlowFile> poll() {
                pollCount.incrementAndGet();
                return List.of(FlowFile.Companion.create(new byte[] {1, 2, 3}, Map.of("seq",
                        Integer.toString(pollCount.get()))));
            }
            @Override protected void onIngested(FlowFile ff) {
                ingestedIds.add(ff.getId());
                ingestedOnce.countDown();
            }
        };

        assertFalse(source.isRunning());
        List<FlowFile> delivered = new CopyOnWriteArrayList<>();
        source.start(ff -> { delivered.add(ff); return true; });
        assertTrue(source.isRunning());

        assertTrue(ingestedOnce.await(2, TimeUnit.SECONDS), "poll loop never emitted");
        source.stop();
        assertFalse(source.isRunning());

        assertFalse(delivered.isEmpty());
        assertEquals(delivered.size(), ingestedIds.size(),
                "onIngested must fire once per accepted FlowFile");
    }

    @Test
    void rejectedFlowFilesHitOnRejected() throws Exception {
        CountDownLatch rejected = new CountDownLatch(1);
        PollingSource source = new PollingSource("probe", 10) {

            @Override public boolean isRunning() { return getRunning(); }
            @Override public String sourceType() { return "probe"; }
            @Override protected List<FlowFile> poll() {
                return List.of(FlowFile.Companion.create(new byte[0], Map.of()));
            }
            @Override protected void onRejected(FlowFile ff) { rejected.countDown(); }
        };
        source.start(ff -> false);
        assertTrue(rejected.await(2, TimeUnit.SECONDS));
        source.stop();
    }

    @Test
    void nonPositivePollIntervalFallsBackToDefault() {
        PollingSource source = new PollingSource("probe", 0) {
            @Override public boolean isRunning() { return getRunning(); }
            @Override public String sourceType() { return "probe"; }
            @Override protected List<FlowFile> poll() { return List.of(); }
        };
        assertEquals(1000, source.pollIntervalMillis());
    }

    @Test
    void doubleStartIsIdempotent() {
        PollingSource source = new PollingSource("probe", 50) {
            @Override public boolean isRunning() { return getRunning(); }
            @Override public String sourceType() { return "probe"; }
            @Override protected List<FlowFile> poll() { return List.of(); }
        };
        source.start(ff -> true);
        source.start(ff -> true); // no throw
        assertTrue(source.isRunning());
        source.stop();
    }
}
