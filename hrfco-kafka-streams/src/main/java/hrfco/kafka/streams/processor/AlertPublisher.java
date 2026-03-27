package hrfco.kafka.streams.processor;

import hrfco.kafka.streams.model.WaterLevelData;
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
 * Kafka 알림 토픽으로 경보 메시지 발행
 */
public class AlertPublisher {
    private static final Logger logger = LoggerFactory.getLogger(AlertPublisher.class);

    private static final String ALERT_TOPIC = System.getenv()
        .getOrDefault("ALERT_TOPIC", "hrfco-alerts");

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    public AlertPublisher(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
        this.objectMapper = new ObjectMapper();

        logger.info("AlertPublisher initialized. Topic: {}", ALERT_TOPIC);
    }

    /**
     * 경보 알림 발송
     */
    public void publishAlert(WaterLevelData data, String warningLevel, boolean isAnomaly) {
        try {
            Map<String, Object> alert = new HashMap<>();
            alert.put("observationCode", data.getObservationCode());
            alert.put("observationTime", data.getFormattedObservationTime());
            alert.put("waterLevel", data.getWaterLevelValue());
            alert.put("flowRate", data.getFlowRateValue());
            alert.put("warningLevel", warningLevel);
            alert.put("isAnomaly", isAnomaly);

            String message;
            if (isAnomaly) {
                message = String.format("관측소 %s에서 이상치 감지 (수위: %.2f m)",
                    data.getObservationCode(), data.getWaterLevelValue());
            } else {
                message = String.format("관측소 %s에서 %s 수위 경보 (수위: %.2f m)",
                    data.getObservationCode(), warningLevel, data.getWaterLevelValue());
            }
            alert.put("message", message);
            alert.put("timestamp", System.currentTimeMillis());

            String alertJson = objectMapper.writeValueAsString(alert);

            ProducerRecord<String, String> record = new ProducerRecord<>(
                ALERT_TOPIC, data.getObservationCode(), alertJson);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to send alert for observation: {}",
                        data.getObservationCode(), exception);
                } else {
                    logger.info("Alert sent: {} - {} (partition: {}, offset: {})",
                        warningLevel, data.getObservationCode(),
                        metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            logger.error("Failed to create alert message", e);
        }
    }

    public void close() {
        if (producer != null) {
            producer.close();
            logger.info("AlertPublisher closed");
        }
    }
}
