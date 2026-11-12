package hrfco.kafka.streams.dlq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * DLQ 봉투 (DeadLetterQueueProducer가 만든 JSON)
 */
public record DlqEnvelope(String originalKey, String originalValue, String errorReason) {

    public static DlqEnvelope parse(ObjectMapper mapper, String json) throws IOException {
        JsonNode node = mapper.readTree(json);
        if (node == null || !node.isObject()) {
            throw new IOException("DLQ envelope is not a JSON object");
        }
        return new DlqEnvelope(
            text(node, "originalKey"),
            text(node, "originalValue"),
            text(node, "errorReason"));
    }

    /**
     * 저장 실패로 DLQ에 온 레코드인가.
     * 저장 실패는 저장소가 회복되면 성공할 수 있지만, 파싱 실패 등 처리 실패는 다시 해도 같은 이유로 실패한다.
     */
    public boolean isStorageFailure() {
        return errorReason != null
            && (errorReason.startsWith(DeadLetterQueueProducer.REASON_PARTIAL_STORAGE_FAILURE)
                || errorReason.startsWith(DeadLetterQueueProducer.REASON_ALL_STORAGES_FAILED));
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }
}
