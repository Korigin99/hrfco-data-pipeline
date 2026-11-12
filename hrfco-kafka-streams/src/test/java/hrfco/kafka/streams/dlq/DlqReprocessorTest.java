package hrfco.kafka.streams.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import hrfco.kafka.streams.service.StorageResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class DlqReprocessorTest {

    private static final String TOPIC = "hrfco-waterlevel-dlq";
    private final TopicPartition tp = new TopicPartition(TOPIC, 0);
    private final ObjectMapper mapper = new ObjectMapper();

    private MockConsumer<String, String> consumer;
    private final List<String> restored = new ArrayList<>();
    private StorageResult nextResult;

    @BeforeEach
    void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.assign(List.of(tp));
        consumer.updateBeginningOffsets(Map.of(tp, 0L));
        nextResult = new StorageResult();   // 기본: 세 저장소 모두 성공
    }

    private DlqReprocessor reprocessor() {
        return new DlqReprocessor(consumer, TOPIC, value -> {
            restored.add(value);
            return nextResult;
        }, Duration.ZERO);
    }

    @Test
    void storageFailure_restoredToAllStores_thenCommitted() throws Exception {
        consumer.addRecord(record(0, "Partial storage failure: MinIO", "v0"));

        reprocessor().pollOnce();

        assertEquals(List.of("v0"), restored);
        assertEquals(1L, committedOffset());
    }

    @Test
    void stillFailing_notCommitted_andRewoundToSameRecord() throws Exception {
        nextResult.markMinIOFailure("down");
        consumer.addRecord(record(0, "Partial storage failure: MinIO", "v0"));
        consumer.addRecord(record(1, "Partial storage failure: MinIO", "v1"));

        reprocessor().pollOnce();

        // 실패한 레코드에서 멈추고 뒤 레코드는 건드리지 않는다 (순서 유지, 건너뛰기 없음)
        assertEquals(List.of("v0"), restored);
        assertNull(committedOffset());
        assertEquals(0L, consumer.position(tp));
    }

    @Test
    void processingFailure_skippedWithoutRestore_andCommitted() throws Exception {
        consumer.addRecord(record(0, "Processing failed: JsonParseException", "broken"));

        reprocessor().pollOnce();

        assertTrue(restored.isEmpty());
        assertEquals(1L, committedOffset());
    }

    @Test
    void malformedEnvelope_skipped_andCommitted() throws Exception {
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, "k", "not-json"));

        reprocessor().pollOnce();

        assertTrue(restored.isEmpty());
        assertEquals(1L, committedOffset());
    }

    @Test
    void restoreThrows_skipped_andCommitted() throws Exception {
        consumer.addRecord(record(0, "Partial storage failure: MinIO", "v0"));
        DlqReprocessor r = new DlqReprocessor(consumer, TOPIC, value -> {
            throw new IllegalArgumentException("invalid record");
        }, Duration.ZERO);

        r.pollOnce();

        assertEquals(1L, committedOffset());
    }

    @Test
    void kafkaError_doesNotKillLoop_andNextStepContinues() throws Exception {
        // 브로커 재시작 중 poll·commit 타임아웃 등으로 재처리 스레드가 죽으면 DLQ가 영영 처리되지 않는다
        DlqReprocessor r = reprocessor();
        consumer.setPollException(new org.apache.kafka.common.KafkaException("broker unavailable"));

        assertDoesNotThrow(r::step);

        consumer.addRecord(record(0, "Partial storage failure: MinIO", "v0"));
        r.step();
        assertEquals(List.of("v0"), restored);
        assertEquals(1L, committedOffset());
    }

    private Long committedOffset() {
        OffsetAndMetadata om = consumer.committed(Set.of(tp)).get(tp);
        return om == null ? null : om.offset();
    }

    private ConsumerRecord<String, String> record(long offset, String reason, String originalValue) throws Exception {
        String envelope = mapper.writeValueAsString(Map.of(
            "originalKey", "1001602",
            "originalValue", originalValue,
            "errorReason", reason,
            "timestamp", 1L));
        return new ConsumerRecord<>(TOPIC, 0, offset, "1001602", envelope);
    }
}
