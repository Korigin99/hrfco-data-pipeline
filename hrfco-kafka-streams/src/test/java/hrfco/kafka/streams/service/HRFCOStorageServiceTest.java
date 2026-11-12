package hrfco.kafka.streams.service;

import hrfco.kafka.streams.model.WaterLevelData;
import hrfco.kafka.streams.repository.MinIORepository;
import hrfco.kafka.streams.repository.MongoDBRepository;
import hrfco.kafka.streams.repository.TimescaleDBRepository;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 저장 서비스가 세 저장소 모두 멱등 키(관측소 코드 + 관측 시각) 위치에 쓰는지 확인한다.
 * 실제 덮어쓰기 동작(ON CONFLICT, upsert, 같은 경로 PUT)은 Docker 실험으로 검증한다.
 */
class HRFCOStorageServiceTest {

    private static final String JSON =
        "{\"wlobscd\":\"1001602\",\"ymdhm\":\"202611121530\",\"wl\":\"1.23\",\"fw\":\"10.5\"}";

    private TimescaleDBRepository timescaleDB;
    private MinIORepository minIO;
    private MongoDBRepository mongoDB;
    private HRFCOStorageService service;

    @BeforeEach
    void setUp() {
        timescaleDB = mock(TimescaleDBRepository.class);
        minIO = mock(MinIORepository.class);
        mongoDB = mock(MongoDBRepository.class);
        service = new HRFCOStorageService(timescaleDB, minIO, mongoDB);
    }

    @AfterEach
    void tearDown() {
        service.shutdown();
    }

    @Test
    void minIO_writesToObservationKeyPath_andRemovesCounterpart() {
        StorageResult result = service.saveWaterLevelData(data(), JSON, false, "NORMAL");

        assertTrue(result.isAllSuccess());
        verify(minIO).saveJson(eq("waterlevel/normal/2026/11/12/obs_1001602_202611121530.json"), anyString());
        verify(minIO).deleteObject("waterlevel/anomalies/2026/11/12/obs_1001602_202611121530.json");
    }

    @Test
    void mongoDB_upsertsByObservationKeyId() {
        service.saveWaterLevelData(data(), JSON, true, null);

        ArgumentCaptor<Document> doc = ArgumentCaptor.forClass(Document.class);
        verify(mongoDB).upsertDocument(eq("water_level_data"), eq("1001602_202611121530"), doc.capture());
        assertEquals("1.23", doc.getValue().getString("wl"));
        assertEquals(true, doc.getValue().getBoolean("is_anomaly"));
    }

    @Test
    void timescaleDB_upsertsWithObservationTimeInKst() {
        service.saveWaterLevelData(data(), JSON, false, "NORMAL");

        Timestamp expected = Timestamp.from(
            ZonedDateTime.of(2026, 11, 12, 15, 30, 0, 0, ZoneId.of("Asia/Seoul")).toInstant());
        verify(timescaleDB).upsertWaterLevelData(
            eq("1001602"), eq(expected), eq(1.23), eq(10.5), eq(false), eq("NORMAL"));
    }

    private WaterLevelData data() {
        WaterLevelData d = new WaterLevelData();
        d.setObservationCode("1001602");
        d.setObservationTime("202611121530");
        d.setWaterLevel("1.23");
        d.setFlowRate("10.5");
        return d;
    }
}
