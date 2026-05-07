package zincflow.core;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

final class ContentStoreCleanupTest {

    @Test
    void sweepDeletesOrphanedClaims() {
        var store = new MemoryContentStore();
        var cleanup = new ContentStoreCleanup(store);

        String a = store.store("a".getBytes());
        String b = store.store("b".getBytes());
        String c = store.store("c".getBytes());

        cleanup.track(a);
        cleanup.track(c);
        // b is orphaned — not tracked
        int deleted = cleanup.sweep(List.of(a, b, c));
        assertEquals(1, deleted);
        assertFalse(store.exists(b));
        assertTrue(store.exists(a));
        assertTrue(store.exists(c));
    }

    @Test
    void releaseMakesAClaimEligibleForSweep() {
        var store = new MemoryContentStore();
        var cleanup = new ContentStoreCleanup(store);
        String id = store.store("x".getBytes());
        cleanup.track(id);

        assertEquals(0, cleanup.sweep(List.of(id)),
                "tracked claim shouldn't be swept");

        cleanup.release(id);
        assertEquals(1, cleanup.sweep(List.of(id)));
        assertFalse(store.exists(id));
    }

    @Test
    void activeCountTracksOutstandingTracks() {
        var cleanup = new ContentStoreCleanup(new MemoryContentStore());
        assertEquals(0, cleanup.activeCount());
        cleanup.track("a");
        cleanup.track("b");
        assertEquals(2, cleanup.activeCount());
        cleanup.release("a");
        assertEquals(1, cleanup.activeCount());
    }

    @Test
    void periodicSweepRunsRepeatedly() throws Exception {
        var store = new MemoryContentStore();
        var cleanup = new ContentStoreCleanup(store);
        var sweeps = new AtomicInteger();
        var latch = new CountDownLatch(2);

        cleanup.startPeriodicSweep(50, TimeUnit.MILLISECONDS, () -> {
            sweeps.incrementAndGet();
            latch.countDown();
            return List.of();
        });
        assertTrue(latch.await(2, TimeUnit.SECONDS),
                "expected at least two sweeps, saw " + sweeps.get());
        cleanup.stopPeriodicSweep();
    }

    @Test
    void stopPeriodicSweepIsIdempotent() {
        var cleanup = new ContentStoreCleanup(new MemoryContentStore());
        // Never started — stop should be a no-op, not a throw.
        cleanup.stopPeriodicSweep();
        cleanup.stopPeriodicSweep();
    }
}
