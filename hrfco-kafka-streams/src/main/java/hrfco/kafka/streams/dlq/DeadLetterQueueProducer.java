package hrfco.kafka.streams.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Dead Letter Queue Producer
 * 처리 실패한 레코드를 DLQ 토픽으로 전송하여 나중에 재처리 가능하도록 함
 */
public class DeadLetterQueueProducer {
    private static final Logger logger = LoggerFactory.getLogger(DeadLetterQueueProducer.class);

    private static final String DLQ_TOPIC = System.getenv()
        .getOrDefault("DLQ_TOPIC", "hrfco-waterlevel-dlq");

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    public DeadLetterQueueProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        this.producer = new KafkaProducer<>(props);
        this.objectMapper = new ObjectMapper();

        logger.info("DeadLetterQueueProducer initialized. DLQ topic: {}", DLQ_TOPIC);
    }

    /**
     * 처리 실패 레코드를 DLQ로 전송
     *
     * @param originalKey 원본 Kafka 메시지 키
     * @param originalValue 원본 Kafka 메시지 값
     * @param errorReason 실패 원인 설명
     * @param exception 발생한 예외
     */
    public void send(String originalKey, String originalValue, String errorReason, Exception exception) {
        try {
            Map<String, Object> envelope = new HashMap<>();
            envelope.put("originalKey", originalKey);
            envelope.put("originalValue", originalValue);
            envelope.put("errorReason", errorReason);
            envelope.put("exceptionClass", exception != null ? exception.getClass().getName() : null);
            envelope.put("exceptionMessage", exception != null ? exception.getMessage() : null);
            envelope.put("timestamp", System.currentTimeMillis());

            String envelopeJson = objectMapper.writeValueAsString(envelope);

            ProducerRecord<String, String> record = new ProducerRecord<>(DLQ_TOPIC, originalKey, envelopeJson);

            producer.send(record, (metadata, ex) -> {
                if (ex != null) {
                    logger.error("Failed to send to DLQ - Key: {}, Reason: {}", originalKey, errorReason, ex);
                } else {
                    logger.warn("Record sent to DLQ - Key: {}, Reason: {}, Topic: {}, Partition: {}, Offset: {}",
                        originalKey, errorReason, metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            logger.error("Failed to create DLQ envelope - Key: {}, Reason: {}", originalKey, errorReason, e);
        }
    }

    /**
     * 부분 저장 실패 레코드를 DLQ로 전송
     *
     * @param originalKey 원본 키
     * @param originalValue 원본 값
     * @param failedStorages 실패한 저장소 목록
     */
    public void sendPartialFailure(String originalKey, String originalValue, java.util.List<String> failedStorages) {
        String reason = "Partial storage failure: " + String.join(", ", failedStorages);
        send(originalKey, originalValue, reason, null);
    }

    public void close() {
        if (producer != null) {
            producer.close();
            logger.info("DeadLetterQueueProducer closed");
        }
    }
}
