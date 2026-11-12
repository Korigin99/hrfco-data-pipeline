package hrfco.kafka.streams.processor;

import hrfco.kafka.streams.dlq.DeadLetterQueueProducer;
import hrfco.kafka.streams.model.WaterLevelData;
import hrfco.kafka.streams.service.HRFCOStorageService;
import hrfco.kafka.streams.service.StorageResult;
import hrfco.kafka.streams.validator.WaterLevelValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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

            // 1~3. 파싱 → 필수 필드 검증 → 범위 검증·이상치 탐지
            Analyzed analyzed = analyze(value);
            if (analyzed == null) {
                logger.error("Required fields validation failed. Skipping. Value: {}", value);
                return;
            }
            WaterLevelData data = analyzed.data();
            boolean isAnomaly = analyzed.isAnomaly();
            String floodWarningLevel = analyzed.floodWarningLevel();

            // 4. 저장
            StorageResult storageResult = storageService.saveWaterLevelData(
                data, value, isAnomaly, floodWarningLevel);

            // 저장 실패 시 DLQ로 전송
            if (storageResult.isPartialFailure()) {
                dlqProducer.sendPartialFailure(key, value, storageResult.getFailures());
            } else if (storageResult.isAllFailed()) {
                dlqProducer.send(key, value,
                    DeadLetterQueueProducer.REASON_ALL_STORAGES_FAILED + ": " + storageResult, null);
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

    /**
     * DLQ 재처리: 저장만 다시 한다.
     * 경보는 처음 처리할 때 이미 보냈으므로 다시 보내지 않고, 실패해도 DLQ로 다시 보내지 않는다(재처리기가 판단).
     *
     * @throws IllegalArgumentException 필수 필드가 없어 다시 해도 저장할 수 없는 레코드
     */
    public StorageResult restore(String value) throws IOException {
        Analyzed analyzed = analyze(value);
        if (analyzed == null) {
            throw new IllegalArgumentException("Required fields validation failed: " + value);
        }
        return storageService.saveWaterLevelData(
            analyzed.data(), value, analyzed.isAnomaly(), analyzed.floodWarningLevel());
    }

    private record Analyzed(WaterLevelData data, boolean isAnomaly, String floodWarningLevel) {
    }

    /**
     * 파싱 → 필수 필드 검증 → 범위 검증·이상치 탐지
     *
     * @return 필수 필드 검증에 실패하면 null
     */
    private Analyzed analyze(String value) throws IOException {
        WaterLevelData data = objectMapper.readValue(value, WaterLevelData.class);
        logger.info("Parsed - ObsCode: {}, Time: {}, WL: {}, FW: {}",
            data.getObservationCode(),
            data.getFormattedObservationTime(),
            data.getWaterLevelValue(),
            data.getFlowRateValue());

        if (!WaterLevelValidator.validateRequiredFields(data)) {
            return null;
        }

        boolean isRangeValid = WaterLevelValidator.validateDataRange(data);
        AnomalyDetector.AnomalyResult anomalyResult = anomalyDetector.detect(data, isRangeValid);
        return new Analyzed(data, anomalyResult.isAnomaly(), anomalyResult.getFloodWarningLevel());
    }
}
