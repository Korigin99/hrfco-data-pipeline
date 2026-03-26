package hrfco.kafka.streams.cache;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StationThresholdTest {

    @Test
    void fromDocument_allDoubleFields() {
        Document doc = new Document()
            .append("attwl", 3.0)
            .append("wrnwl", 5.0)
            .append("almwl", 7.0)
            .append("srswl", 9.0);

        StationThreshold threshold = StationThreshold.fromDocument(doc);

        assertNotNull(threshold);
        assertEquals(3.0, threshold.getAttentionThreshold());
        assertEquals(5.0, threshold.getCautionThreshold());
        assertEquals(7.0, threshold.getWarningThreshold());
        assertEquals(9.0, threshold.getCriticalThreshold());
    }

    @Test
    void fromDocument_nullDocument_returnsNull() {
        assertNull(StationThreshold.fromDocument(null));
    }

    @Test
    void fromDocument_stringValues_parsedCorrectly() {
        Document doc = new Document()
            .append("attwl", "3.5")
            .append("wrnwl", "5.5")
            .append("almwl", "7.5")
            .append("srswl", "9.5");

        StationThreshold threshold = StationThreshold.fromDocument(doc);

        assertNotNull(threshold);
        assertEquals(3.5, threshold.getAttentionThreshold());
        assertEquals(9.5, threshold.getCriticalThreshold());
    }

    @Test
    void fromDocument_integerValues_convertedToDouble() {
        Document doc = new Document()
            .append("attwl", 3)
            .append("wrnwl", 5)
            .append("almwl", 7)
            .append("srswl", 9);

        StationThreshold threshold = StationThreshold.fromDocument(doc);

        assertNotNull(threshold);
        assertEquals(3.0, threshold.getAttentionThreshold());
        assertEquals(9.0, threshold.getCriticalThreshold());
    }

    @Test
    void fromDocument_emptyStringValues_returnsNull() {
        Document doc = new Document()
            .append("attwl", "")
            .append("wrnwl", " ")
            .append("almwl", null)
            .append("srswl", 9.0);

        StationThreshold threshold = StationThreshold.fromDocument(doc);

        assertNotNull(threshold);
        assertNull(threshold.getAttentionThreshold());
        assertNull(threshold.getCautionThreshold());
        assertNull(threshold.getWarningThreshold());
        assertEquals(9.0, threshold.getCriticalThreshold());
    }

    @Test
    void fromDocument_missingFields_returnsNullThresholds() {
        Document doc = new Document(); // no threshold fields

        StationThreshold threshold = StationThreshold.fromDocument(doc);

        assertNotNull(threshold);
        assertNull(threshold.getAttentionThreshold());
        assertNull(threshold.getCautionThreshold());
        assertNull(threshold.getWarningThreshold());
        assertNull(threshold.getCriticalThreshold());
    }

    @Test
    void fromDocument_invalidStringValue_returnsNull() {
        Document doc = new Document()
            .append("attwl", "not_a_number")
            .append("wrnwl", 5.0)
            .append("almwl", 7.0)
            .append("srswl", 9.0);

        StationThreshold threshold = StationThreshold.fromDocument(doc);

        assertNotNull(threshold);
        assertNull(threshold.getAttentionThreshold());
        assertEquals(5.0, threshold.getCautionThreshold());
    }

    @Test
    void toString_containsAllValues() {
        StationThreshold threshold = new StationThreshold(3.0, 5.0, 7.0, 9.0);
        String result = threshold.toString();
        assertTrue(result.contains("3.0"));
        assertTrue(result.contains("5.0"));
        assertTrue(result.contains("7.0"));
        assertTrue(result.contains("9.0"));
    }
}
