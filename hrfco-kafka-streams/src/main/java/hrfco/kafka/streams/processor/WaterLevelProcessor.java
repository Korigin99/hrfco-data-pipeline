package hrfco.kafka.streams.processor;

import hrfco.kafka.streams.dlq.DeadLetterQueueProducer;
import hrfco.kafka.streams.model.WaterLevelData;
import hrfco.kafka.streams.service.HRFCOStorageService;
import hrfco.kafka.streams.service.StorageResult;
import hrfco.kafka.streams.validator.WaterLevelValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 수위 데이터 처리 프로세서
 * Kafka 레코드를 수신하여 검증 -> 이상치 탐지 -> 저장 -> 알림 발송
 */
public class WaterLevelProcessor {
    private static final Logger logger = LoggerFactory.getLogger(WaterLevelProcessor.class);

    private final ObjectMapper objectMapper;
    private final AnomalyDetector anomalyDetector;
    private final HRFCOStorageService storageService;
    private final AlertPublisher alertPublisher;
    private final DeadLetterQueueProducer dlqProducer;

    public WaterLevelProcessor(ObjectMapper objectMapper,
                               AnomalyDetector anomalyDetector,
                               HRFCOStorageService storageService,
                               AlertPublisher alertPublisher,
                               DeadLetterQueueProducer dlqProducer) {
        this.objectMapper = objectMapper;
        this.anomalyDetector = anomalyDetector;
        this.storageService = storageService;
        this.alertPublisher = alertPublisher;
        this.dlqProducer = dlqProducer;
    }

    /**
     * Kafka 레코드 처리
     */
    public void process(String key, String value) {
        try {
            logger.info("=== Received water level data ===");

            // 1. JSON 파싱
            WaterLevelData data = objectMapper.readValue(value, WaterLevelData.class);
            logger.info("Parsed - ObsCode: {}, Time: {}, WL: {}, FW: {}",
                data.getObservationCode(),
                data.getFormattedObservationTime(),
                data.getWaterLevelValue(),
                data.getFlowRateValue());

            // 2. 필수 필드 검증
            if (!WaterLevelValidator.validateRequiredFields(data)) {
                logger.error("Required fields validation failed for: {}. Skipping.",
                    data.getObservationCode());
                return;
            }

            // 3. 데이터 범위 검증 + 이상치 탐지
            boolean isRangeValid = WaterLevelValidator.validateDataRange(data);
            AnomalyDetector.AnomalyResult anomalyResult = anomalyDetector.detect(data, isRangeValid);

            boolean isAnomaly = anomalyResult.isAnomaly();
            String floodWarningLevel = anomalyResult.getFloodWarningLevel();

            // 4. 저장
            StorageResult storageResult = storageService.saveWaterLevelData(
                data, value, isAnomaly, floodWarningLevel);

            // 저장 실패 시 DLQ로 전송
            if (storageResult.isPartialFailure()) {
                dlqProducer.sendPartialFailure(key, value, storageResult.getFailures());
            } else if (storageResult.isAllFailed()) {
                dlqProducer.send(key, value, "All storages failed: " + storageResult, null);
            }

            // 5. 알림 발송
            if (isAnomaly) {
                alertPublisher.publishAlert(data, "ANOMALY", true);
            } else {
                String alertLevel = floodWarningLevel;
                if ("NO_THRESHOLD".equals(floodWarningLevel) || "PARTIAL_THRESHOLD".equals(floodWarningLevel)) {
                    alertLevel = "NORMAL";
                }
                alertPublisher.publishAlert(data, alertLevel, false);
            }

            logger.info("=== Processing completed ===");

        } catch (Exception e) {
            logger.error("=== ERROR processing water level data ===", e);
            logger.error("Key: {}, Value: {}", key, value);
            dlqProducer.send(key, value, "Processing failed: " + e.getClass().getSimpleName(), e);
        }
    }
}
