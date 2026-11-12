package hrfco.kafka.streams.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DlqEnvelopeTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void parse_readsOriginalValue() throws Exception {
        DlqEnvelope env = DlqEnvelope.parse(mapper, envelope("Partial storage failure: MinIO"));
        assertEquals("1001602", env.originalKey());
        assertEquals("{\"wlobscd\":\"1001602\"}", env.originalValue());
    }

    @Test
    void partialStorageFailure_isStorageFailure() throws Exception {
        assertTrue(DlqEnvelope.parse(mapper, envelope("Partial storage failure: MinIO")).isStorageFailure());
    }

    @Test
    void allStoragesFailed_isStorageFailure() throws Exception {
        assertTrue(DlqEnvelope.parse(mapper,
            envelope("All storages failed: StorageResult{success=0/3}")).isStorageFailure());
    }

    @Test
    void processingFailure_isNotStorageFailure() throws Exception {
        // 파싱 실패 등은 다시 처리해도 같은 이유로 실패한다
        assertFalse(DlqEnvelope.parse(mapper,
            envelope("Processing failed: JsonParseException")).isStorageFailure());
    }

    @Test
    void parse_notJson_throws() {
        assertThrows(Exception.class, () -> DlqEnvelope.parse(mapper, "not-json"));
    }

    private String envelope(String reason) throws Exception {
        return mapper.writeValueAsString(java.util.Map.of(
            "originalKey", "1001602",
            "originalValue", "{\"wlobscd\":\"1001602\"}",
            "errorReason", reason,
            "timestamp", 1L));
    }
}
