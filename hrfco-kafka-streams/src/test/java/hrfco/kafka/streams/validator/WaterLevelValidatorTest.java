package hrfco.kafka.streams.validator;

import hrfco.kafka.streams.cache.StationThreshold;
import hrfco.kafka.streams.model.WaterLevelData;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WaterLevelValidatorTest {

    // === validateRequiredFields ===

    @Test
    void validateRequiredFields_allPresent_returnsTrue() {
        WaterLevelData data = createData("1001602", "202512091530", "1.23", "45.67");
        assertTrue(WaterLevelValidator.validateRequiredFields(data));
    }

    @Test
    void validateRequiredFields_nullData_returnsFalse() {
        assertFalse(WaterLevelValidator.validateRequiredFields(null));
    }

    @Test
    void validateRequiredFields_missingObsCode_returnsFalse() {
        WaterLevelData data = createData(null, "202512091530", "1.23", "45.67");
        assertFalse(WaterLevelValidator.validateRequiredFields(data));
    }

    @Test
    void validateRequiredFields_emptyObsCode_returnsFalse() {
        WaterLevelData data = createData("  ", "202512091530", "1.23", "45.67");
        assertFalse(WaterLevelValidator.validateRequiredFields(data));
    }

    @Test
    void validateRequiredFields_missingTime_returnsFalse() {
        WaterLevelData data = createData("1001602", null, "1.23", "45.67");
        assertFalse(WaterLevelValidator.validateRequiredFields(data));
    }

    @Test
    void validateRequiredFields_bothMeasurementsMissing_returnsFalse() {
        WaterLevelData data = createData("1001602", "202512091530", null, null);
        assertFalse(WaterLevelValidator.validateRequiredFields(data));
    }

    @Test
    void validateRequiredFields_bothMeasurementsEmpty_returnsFalse() {
        WaterLevelData data = createData("1001602", "202512091530", "", "");
        assertFalse(WaterLevelValidator.validateRequiredFields(data));
    }

    @Test
    void validateRequiredFields_onlyWaterLevel_returnsTrue() {
        WaterLevelData data = createData("1001602", "202512091530", "1.23", null);
        assertTrue(WaterLevelValidator.validateRequiredFields(data));
    }

    @Test
    void validateRequiredFields_onlyFlowRate_returnsTrue() {
        WaterLevelData data = createData("1001602", "202512091530", null, "45.67");
        assertTrue(WaterLevelValidator.validateRequiredFields(data));
    }

    // === validateDataRange ===

    @Test
    void validateDataRange_withinRange_returnsTrue() {
        WaterLevelData data = createData("1001602", "202512091530", "5.0", "100.0");
        assertTrue(WaterLevelValidator.validateDataRange(data));
    }

    @Test
    void validateDataRange_waterLevelTooHigh_returnsFalse() {
        WaterLevelData data = createData("1001602", "202512091530", "55.0", "100.0");
        assertFalse(WaterLevelValidator.validateDataRange(data));
    }

    @Test
    void validateDataRange_waterLevelTooLow_returnsFalse() {
        WaterLevelData data = createData("1001602", "202512091530", "-15.0", "100.0");
        assertFalse(WaterLevelValidator.validateDataRange(data));
    }

    @Test
    void validateDataRange_flowRateNegative_returnsFalse() {
        WaterLevelData data = createData("1001602", "202512091530", "5.0", "-1.0");
        assertFalse(WaterLevelValidator.validateDataRange(data));
    }

    @Test
    void validateDataRange_flowRateTooHigh_returnsFalse() {
        WaterLevelData data = createData("1001602", "202512091530", "5.0", "60000.0");
        assertFalse(WaterLevelValidator.validateDataRange(data));
    }

    @Test
    void validateDataRange_nullValues_returnsTrue() {
        WaterLevelData data = createData("1001602", "202512091530", null, null);
        assertTrue(WaterLevelValidator.validateDataRange(data));
    }

    @Test
    void validateDataRange_boundaryMinWaterLevel_returnsTrue() {
        WaterLevelData data = createData("1001602", "202512091530", "-10.0", null);
        assertTrue(WaterLevelValidator.validateDataRange(data));
    }

    @Test
    void validateDataRange_boundaryMaxWaterLevel_returnsTrue() {
        WaterLevelData data = createData("1001602", "202512091530", "50.0", null);
        assertTrue(WaterLevelValidator.validateDataRange(data));
    }

    // === getFloodWarningLevel ===

    @Test
    void getFloodWarningLevel_critical() {
        WaterLevelData data = createData("1001602", "202512091530", "10.0", null);
        StationThreshold threshold = new StationThreshold(3.0, 5.0, 7.0, 9.0);
        assertEquals("CRITICAL", WaterLevelValidator.getFloodWarningLevel(data, threshold));
    }

    @Test
    void getFloodWarningLevel_warning() {
        WaterLevelData data = createData("1001602", "202512091530", "8.0", null);
        StationThreshold threshold = new StationThreshold(3.0, 5.0, 7.0, 9.0);
        assertEquals("WARNING", WaterLevelValidator.getFloodWarningLevel(data, threshold));
    }

    @Test
    void getFloodWarningLevel_caution() {
        WaterLevelData data = createData("1001602", "202512091530", "6.0", null);
        StationThreshold threshold = new StationThreshold(3.0, 5.0, 7.0, 9.0);
        assertEquals("CAUTION", WaterLevelValidator.getFloodWarningLevel(data, threshold));
    }

    @Test
    void getFloodWarningLevel_attention() {
        WaterLevelData data = createData("1001602", "202512091530", "4.0", null);
        StationThreshold threshold = new StationThreshold(3.0, 5.0, 7.0, 9.0);
        assertEquals("ATTENTION", WaterLevelValidator.getFloodWarningLevel(data, threshold));
    }

    @Test
    void getFloodWarningLevel_normal() {
        WaterLevelData data = createData("1001602", "202512091530", "2.0", null);
        StationThreshold threshold = new StationThreshold(3.0, 5.0, 7.0, 9.0);
        assertEquals("NORMAL", WaterLevelValidator.getFloodWarningLevel(data, threshold));
    }

    @Test
    void getFloodWarningLevel_nullThreshold_returnsNull() {
        WaterLevelData data = createData("1001602", "202512091530", "5.0", null);
        assertNull(WaterLevelValidator.getFloodWarningLevel(data, null));
    }

    @Test
    void getFloodWarningLevel_allThresholdsNull_returnsNull() {
        WaterLevelData data = createData("1001602", "202512091530", "5.0", null);
        StationThreshold threshold = new StationThreshold(null, null, null, null);
        assertNull(WaterLevelValidator.getFloodWarningLevel(data, threshold));
    }

    @Test
    void getFloodWarningLevel_nullWaterLevel_returnsNull() {
        WaterLevelData data = createData("1001602", "202512091530", null, "100.0");
        StationThreshold threshold = new StationThreshold(3.0, 5.0, 7.0, 9.0);
        assertNull(WaterLevelValidator.getFloodWarningLevel(data, threshold));
    }

    @Test
    void getFloodWarningLevel_partialThresholds_onlyCritical() {
        WaterLevelData data = createData("1001602", "202512091530", "10.0", null);
        StationThreshold threshold = new StationThreshold(null, null, null, 9.0);
        assertEquals("CRITICAL", WaterLevelValidator.getFloodWarningLevel(data, threshold));
    }

    @Test
    void getFloodWarningLevel_partialThresholds_belowCritical_returnsNormal() {
        WaterLevelData data = createData("1001602", "202512091530", "5.0", null);
        StationThreshold threshold = new StationThreshold(null, null, null, 9.0);
        assertEquals("NORMAL", WaterLevelValidator.getFloodWarningLevel(data, threshold));
    }

    @Test
    void getFloodWarningLevel_exactThreshold_returnsThatLevel() {
        WaterLevelData data = createData("1001602", "202512091530", "9.0", null);
        StationThreshold threshold = new StationThreshold(3.0, 5.0, 7.0, 9.0);
        assertEquals("CRITICAL", WaterLevelValidator.getFloodWarningLevel(data, threshold));
    }

    // === Helper ===

    private WaterLevelData createData(String obsCode, String obsTime, String wl, String fw) {
        WaterLevelData data = new WaterLevelData();
        data.setObservationCode(obsCode);
        data.setObservationTime(obsTime);
        data.setWaterLevel(wl);
        data.setFlowRate(fw);
        return data;
    }
}
