package hrfco.kafka.streams.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StationThresholdCacheTest {

    @BeforeEach
    void setUp() {
        StationThresholdCache.clear();
    }

    @Test
    void putAndGet_returnsStoredThreshold() {
        StationThreshold threshold = new StationThreshold(3.0, 5.0, 7.0, 9.0);
        StationThresholdCache.put("1001602", threshold);

        StationThreshold result = StationThresholdCache.get("1001602");
        assertNotNull(result);
        assertEquals(3.0, result.getAttentionThreshold());
        assertEquals(9.0, result.getCriticalThreshold());
    }

    @Test
    void get_notFound_returnsNull() {
        assertNull(StationThresholdCache.get("UNKNOWN"));
    }

    @Test
    void put_nullThreshold_notStored() {
        StationThresholdCache.put("1001602", null);
        assertNull(StationThresholdCache.get("1001602"));
        assertEquals(0, StationThresholdCache.size());
    }

    @Test
    void clear_removesAllEntries() {
        StationThresholdCache.put("1001602", new StationThreshold(1.0, 2.0, 3.0, 4.0));
        StationThresholdCache.put("1001603", new StationThreshold(1.0, 2.0, 3.0, 4.0));
        assertEquals(2, StationThresholdCache.size());

        StationThresholdCache.clear();
        assertEquals(0, StationThresholdCache.size());
    }

    @Test
    void size_returnsCorrectCount() {
        assertEquals(0, StationThresholdCache.size());
        StationThresholdCache.put("A", new StationThreshold(1.0, 2.0, 3.0, 4.0));
        assertEquals(1, StationThresholdCache.size());
        StationThresholdCache.put("B", new StationThreshold(1.0, 2.0, 3.0, 4.0));
        assertEquals(2, StationThresholdCache.size());
    }

    @Test
    void remove_deletesEntry() {
        StationThresholdCache.put("1001602", new StationThreshold(1.0, 2.0, 3.0, 4.0));
        assertNotNull(StationThresholdCache.get("1001602"));

        StationThresholdCache.remove("1001602");
        assertNull(StationThresholdCache.get("1001602"));
    }
}
