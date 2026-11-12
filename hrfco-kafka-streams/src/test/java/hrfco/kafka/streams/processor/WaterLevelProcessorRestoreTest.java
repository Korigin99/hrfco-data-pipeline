package hrfco.kafka.streams.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import hrfco.kafka.streams.dlq.DeadLetterQueueProducer;
import hrfco.kafka.streams.repository.MongoDBRepository;
import hrfco.kafka.streams.service.HRFCOStorageService;
import hrfco.kafka.streams.service.StorageResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * DLQ 재처리용 restore(): 저장만 다시 하고 경보·DLQ 발행은 하지 않는다
 */
class WaterLevelProcessorRestoreTest {

    private static final String VALID =
        "{\"wlobscd\":\"1001602\",\"ymdhm\":\"202611121530\",\"wl\":\"1.23\",\"fw\":\"10.5\"}";

    private HRFCOStorageService storage;
    private AlertPublisher alerts;
    private DeadLetterQueueProducer dlq;
    private WaterLevelProcessor processor;

    @BeforeEach
    void setUp() {
        storage = mock(HRFCOStorageService.class);
        alerts = mock(AlertPublisher.class);
        dlq = mock(DeadLetterQueueProducer.class);
        AnomalyDetector detector = new AnomalyDetector(mock(MongoDBRepository.class));
        processor = new WaterLevelProcessor(new ObjectMapper(), detector, storage, alerts, dlq);
    }

    @Test
    void restore_savesAgain_withoutAlertOrDlq() throws Exception {
        StorageResult ok = new StorageResult();
        when(storage.saveWaterLevelData(any(), eq(VALID), eq(false), anyString())).thenReturn(ok);

        assertSame(ok, processor.restore(VALID));
        verifyNoInteractions(alerts, dlq);
    }

    @Test
    void restore_invalidRecord_throws() {
        String noTime = "{\"wlobscd\":\"1001602\",\"wl\":\"1.23\"}";
        assertThrows(IllegalArgumentException.class, () -> processor.restore(noTime));
        verifyNoInteractions(storage);
    }
}
