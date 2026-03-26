package hrfco.kafka.streams.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WaterLevelDataTest {

    // === getWaterLevelValue ===

    @Test
    void getWaterLevelValue_validNumber() {
        WaterLevelData data = new WaterLevelData();
        data.setWaterLevel("1.23");
        assertEquals(1.23, data.getWaterLevelValue(), 0.001);
    }

    @Test
    void getWaterLevelValue_negativeNumber() {
        WaterLevelData data = new WaterLevelData();
        data.setWaterLevel("-5.5");
        assertEquals(-5.5, data.getWaterLevelValue(), 0.001);
    }

    @Test
    void getWaterLevelValue_null_returnsNull() {
        WaterLevelData data = new WaterLevelData();
        data.setWaterLevel(null);
        assertNull(data.getWaterLevelValue());
    }

    @Test
    void getWaterLevelValue_empty_returnsNull() {
        WaterLevelData data = new WaterLevelData();
        data.setWaterLevel("");
        assertNull(data.getWaterLevelValue());
    }

    @Test
    void getWaterLevelValue_whitespace_returnsNull() {
        WaterLevelData data = new WaterLevelData();
        data.setWaterLevel("   ");
        assertNull(data.getWaterLevelValue());
    }

    @Test
    void getWaterLevelValue_singleSpace_returnsNull() {
        WaterLevelData data = new WaterLevelData();
        data.setWaterLevel(" ");
        assertNull(data.getWaterLevelValue());
    }

    @Test
    void getWaterLevelValue_invalidNumber_returnsNull() {
        WaterLevelData data = new WaterLevelData();
        data.setWaterLevel("abc");
        assertNull(data.getWaterLevelValue());
    }

    // === getFlowRateValue ===

    @Test
    void getFlowRateValue_validNumber() {
        WaterLevelData data = new WaterLevelData();
        data.setFlowRate("45.67");
        assertEquals(45.67, data.getFlowRateValue(), 0.001);
    }

    @Test
    void getFlowRateValue_null_returnsNull() {
        WaterLevelData data = new WaterLevelData();
        data.setFlowRate(null);
        assertNull(data.getFlowRateValue());
    }

    // === getFormattedObservationTime ===

    @Test
    void getFormattedObservationTime_valid12Digits() {
        WaterLevelData data = new WaterLevelData();
        data.setObservationTime("202512091530");
        assertEquals("2025-12-09 15:30", data.getFormattedObservationTime());
    }

    @Test
    void getFormattedObservationTime_invalidLength_returnsOriginal() {
        WaterLevelData data = new WaterLevelData();
        data.setObservationTime("20251209");
        assertEquals("20251209", data.getFormattedObservationTime());
    }

    @Test
    void getFormattedObservationTime_null_returnsNull() {
        WaterLevelData data = new WaterLevelData();
        data.setObservationTime(null);
        assertNull(data.getFormattedObservationTime());
    }

    // === toString ===

    @Test
    void toString_containsAllFields() {
        WaterLevelData data = new WaterLevelData();
        data.setObservationCode("1001602");
        data.setObservationTime("202512091530");
        data.setWaterLevel("1.23");
        data.setFlowRate("45.67");

        String result = data.toString();
        assertTrue(result.contains("1001602"));
        assertTrue(result.contains("202512091530"));
        assertTrue(result.contains("1.23"));
        assertTrue(result.contains("45.67"));
    }
}
