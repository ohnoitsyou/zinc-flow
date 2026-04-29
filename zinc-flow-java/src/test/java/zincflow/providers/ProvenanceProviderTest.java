package zincflow.providers;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

final class ProvenanceProviderTest {

    @Test
    void recordIsNoOpWhenDisabled() {
        var prov = new ProvenanceProvider();
        // starts DISABLED — record() must silently drop
        prov.record(1, ProvenanceProvider.EventType.PROCESSED, "proc", "ignored");
        assertEquals(0, prov.size());
        assertTrue(prov.getRecent(10).isEmpty());
    }

    @Test
    void recordsCaptureAllFieldsWhenEnabled() {
        var prov = new ProvenanceProvider();
        prov.enable();
        prov.record(42, ProvenanceProvider.EventType.ROUTED, "router", "→ high");
        List<ProvenanceProvider.Event> events = prov.getEvents(42);
        assertEquals(1, events.size());
        var e = events.get(0);
        assertEquals(42, e.flowFileId);
        assertEquals(ProvenanceProvider.EventType.ROUTED, e.type);
        assertEquals("router", e.component);
        assertEquals("→ high", e.details);
        assertTrue(e.timestampMillis > 0);
    }

    @Test
    void getEventsFiltersByFlowFileId() {
        var prov = new ProvenanceProvider();
        prov.enable();
        prov.record(1, ProvenanceProvider.EventType.CREATED, "ingress");
        prov.record(2, ProvenanceProvider.EventType.CREATED, "ingress");
        prov.record(1, ProvenanceProvider.EventType.PROCESSED, "stage");
        assertEquals(2, prov.getEvents(1).size());
        assertEquals(1, prov.getEvents(2).size());
        assertEquals(0, prov.getEvents(99).size());
    }

    @Test
    void getRecentReturnsMostRecentWindow() {
        var prov = new ProvenanceProvider();
        prov.enable();
        for (int i = 0; i < 10; i++) {
            prov.record(i, ProvenanceProvider.EventType.PROCESSED, "p" + i);
        }
        List<ProvenanceProvider.Event> recent = prov.getRecent(3);
        assertEquals(3, recent.size());
        assertEquals(7, recent.get(0).flowFileId);
        assertEquals(8, recent.get(1).flowFileId);
        assertEquals(9, recent.get(2).flowFileId);
    }

    @Test
    void ringBufferEvictsOldestOnOverflow() {
        var prov = new ProvenanceProvider(3);
        prov.enable();
        for (int i = 0; i < 5; i++) {
            prov.record(i, ProvenanceProvider.EventType.PROCESSED, "p");
        }
        assertEquals(3, prov.size());
        // Oldest two (ids 0, 1) evicted; 2, 3, 4 survive
        assertTrue(prov.getEvents(0).isEmpty());
        assertTrue(prov.getEvents(1).isEmpty());
        assertEquals(1, prov.getEvents(2).size());
        assertEquals(1, prov.getEvents(4).size());
    }

    @Test
    void capacityMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> new ProvenanceProvider(0));
        assertThrows(IllegalArgumentException.class, () -> new ProvenanceProvider(-1));
    }
}
