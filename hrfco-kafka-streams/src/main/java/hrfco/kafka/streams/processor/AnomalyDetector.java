package hrfco.kafka.streams.processor;

import hrfco.kafka.streams.cache.StationThreshold;
import hrfco.kafka.streams.cache.StationThresholdCache;
import hrfco.kafka.streams.model.WaterLevelData;
import hrfco.kafka.streams.repository.MongoDBRepository;
import hrfco.kafka.streams.validator.WaterLevelValidator;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 수위 이상치 탐지 및 홍수 경보 수준 판정
 */
public class AnomalyDetector {
    private static final Logger logger = LoggerFactory.getLogger(AnomalyDetector.class);

    private final MongoDBRepository mongoDB;

    public AnomalyDetector(MongoDBRepository mongoDB) {
        this.mongoDB = mongoDB;
    }

    /**
     * 이상치 탐지 결과
     */
    public static class AnomalyResult {
        private final boolean anomaly;
        private final String anomalyReason;
        private final String floodWarningLevel;

        public AnomalyResult(boolean anomaly, String anomalyReason, String floodWarningLevel) {
            this.anomaly = anomaly;
            this.anomalyReason = anomalyReason;
            this.floodWarningLevel = floodWarningLevel;
        }

        public boolean isAnomaly() { return anomaly; }
        public String getAnomalyReason() { return anomalyReason; }
        public String getFloodWarningLevel() { return floodWarningLevel; }
    }

    /**
     * 수위 데이터에 대해 이상치 탐지 및 홍수 경보 수준 판정
     */
    public AnomalyResult detect(WaterLevelData data, boolean isRangeValid) {
        String obsCode = data.getObservationCode();

        // 관측소 임계값 조회 (캐시 우선, 없으면 MongoDB 조회)
        StationThreshold threshold = StationThresholdCache.get(obsCode);

        if (threshold == null) {
            Document stationDoc = mongoDB.getStationThreshold(obsCode);
            if (stationDoc != null) {
                threshold = StationThreshold.fromDocument(stationDoc);
                StationThresholdCache.put(obsCode, threshold);
                logger.debug("Loaded threshold from DB and cached: {}", obsCode);
            } else {
                logger.debug("No threshold found in DB for station: {}", obsCode);
            }
        }

        // 기본 범위를 벗어난 경우는 무조건 이상치
        if (!isRangeValid) {
            logger.warn("Anomaly detected: Data out of physical range for station {}", obsCode);
            return new AnomalyResult(true, "Data out of physical range", null);
        }

        if (threshold == null) {
            logger.info("No threshold data for station: {} - treating as normal", obsCode);
            return new AnomalyResult(false, null, "NO_THRESHOLD");
        }

        // 유효한 임계값 개수 확인
        Double waterLevel = data.getWaterLevelValue();
        if (waterLevel != null) {
            int validCount = countValidThresholds(threshold);

            if (validCount == 0) {
                return new AnomalyResult(false, null, "NO_THRESHOLD");
            } else if (validCount < 2) {
                return new AnomalyResult(false, null, "PARTIAL_THRESHOLD");
            }
        }

        // 홍수 경보 수준 판정
        String floodWarningLevel = WaterLevelValidator.getFloodWarningLevel(data, threshold);
        if (floodWarningLevel == null) {
            floodWarningLevel = "NORMAL";
        }

        logger.info("Station {} - WaterLevel: {} m, Warning: {}, Threshold: {}",
            obsCode, data.getWaterLevelValue(), floodWarningLevel, threshold);

        return new AnomalyResult(false, null, floodWarningLevel);
    }

    private int countValidThresholds(StationThreshold threshold) {
        int count = 0;
        if (threshold.getAttentionThreshold() != null && threshold.getAttentionThreshold() > 0) count++;
        if (threshold.getCautionThreshold() != null && threshold.getCautionThreshold() > 0) count++;
        if (threshold.getWarningThreshold() != null && threshold.getWarningThreshold() > 0) count++;
        if (threshold.getCriticalThreshold() != null && threshold.getCriticalThreshold() > 0) count++;
        return count;
    }
}
