package hrfco.kafka.streams.service;

import hrfco.kafka.streams.model.WaterLevelData;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ObservationKeysTest {

    // === documentId (MongoDB _id) ===

    @Test
    void documentId_joinsCodeAndObservationTime() {
        assertEquals("1001602_202611121530",
            ObservationKeys.documentId(data("1001602", "202611121530")));
    }

    @Test
    void documentId_malformedObservationTime_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> ObservationKeys.documentId(data("1001602", "20261112")));
    }

    // === minioPath ===

    @Test
    void minioPath_normal_usesObservationDateAndTime() {
        assertEquals("waterlevel/normal/2026/11/12/obs_1001602_202611121530.json",
            ObservationKeys.minioPath(data("1001602", "202611121530"), false));
    }

    @Test
    void minioPath_anomaly_goesToAnomaliesFolder() {
        assertEquals("waterlevel/anomalies/2026/11/12/obs_1001602_202611121530.json",
            ObservationKeys.minioPath(data("1001602", "202611121530"), true));
    }

    @Test
    void counterpartMinioPath_isPathOfOppositeFolder() {
        WaterLevelData d = data("1001602", "202611121530");
        assertEquals(ObservationKeys.minioPath(d, true), ObservationKeys.counterpartMinioPath(d, false));
        assertEquals(ObservationKeys.minioPath(d, false), ObservationKeys.counterpartMinioPath(d, true));
    }

    private WaterLevelData data(String code, String ymdhm) {
        WaterLevelData d = new WaterLevelData();
        d.setObservationCode(code);
        d.setObservationTime(ymdhm);
        d.setWaterLevel("1.23");
        return d;
    }
}
