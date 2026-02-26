// 한강 홍수통제소 수위 데이터 검증
// 필수 필드 검증 및 데이터 범위 검증
package hrfco.kafka.streams.validator;

import hrfco.kafka.streams.cache.StationThreshold;
import hrfco.kafka.streams.model.WaterLevelData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WaterLevelValidator {
    private static final Logger logger = LoggerFactory.getLogger(WaterLevelValidator.class);
    
    // 수위 기본 범위 (m) - 한강 기준 범위
    private static final double MIN_WATER_LEVEL = -10.0;  // 최저 수위
    private static final double MAX_WATER_LEVEL = 50.0;   // 최고 수위 (홍수 경보 수위 이상)
    
    // 유량 기본 범위 (CMS)
    private static final double MIN_FLOW_RATE = 0.0;
    private static final double MAX_FLOW_RATE = 50000.0;
    
    /**
     * 필수 필드 검증
     * - 관측소 코드
     * - 관측 시간
     * - 수위 (최소 수위나 유량 중 하나는 있어야 함)
     */
    public static boolean validateRequiredFields(WaterLevelData data) {
        if (data == null) {
            logger.error("WaterLevelData is null");
            return false;
        }
        
        // 관측소 코드 검증
        if (data.getObservationCode() == null || data.getObservationCode().trim().isEmpty()) {
            logger.error("Observation code is missing");
            return false;
        }
        
        // 관측 시간 검증
        if (data.getObservationTime() == null || data.getObservationTime().trim().isEmpty()) {
            logger.error("Observation time is missing");
            return false;
        }
        
        // 최소한 수위나 유량 중 하나는 있어야 함
        if ((data.getWaterLevel() == null || data.getWaterLevel().trim().isEmpty()) &&
            (data.getFlowRate() == null || data.getFlowRate().trim().isEmpty())) {
            logger.error("No measurement values available (water level and flow rate both missing)");
            return false;
        }
        
        logger.info("Required fields validation passed for observation: {}", data.getObservationCode());
        return true;
    }
    
    /**
     * 데이터 범위 검증
     * 수위, 유량의 기본 범위 내에 있는지 검증
     */
    public static boolean validateDataRange(WaterLevelData data) {
        boolean isValid = true;
        
        // 수위 범위 검증
        Double waterLevel = data.getWaterLevelValue();
        if (waterLevel != null) {
            if (waterLevel < MIN_WATER_LEVEL || waterLevel > MAX_WATER_LEVEL) {
                logger.warn("Water level out of range: {} (valid range: {} - {})", 
                        waterLevel, MIN_WATER_LEVEL, MAX_WATER_LEVEL);
                isValid = false;
            }
        }
        
        // 유량 범위 검증
        Double flowRate = data.getFlowRateValue();
        if (flowRate != null) {
            if (flowRate < MIN_FLOW_RATE || flowRate > MAX_FLOW_RATE) {
                logger.warn("Flow rate out of range: {} (valid range: {} - {})", 
                        flowRate, MIN_FLOW_RATE, MAX_FLOW_RATE);
                isValid = false;
            }
        }
        
        if (isValid) {
            logger.info("Data range validation passed for observation: {}", data.getObservationCode());
        } else {
            logger.warn("Data range validation failed for observation: {}", data.getObservationCode());
        }
        
        return isValid;
    }
    
    /**
     * 홍수 경보 수준 판정
     * DB에서 가져온 관측소별 임계값 사용, 임계값이 null이면 null 반환 (경보 판정 스킵)
     * 
     * @param data 수위 데이터
     * @param threshold 관측소별 임계값(null 가능)
     * @return 경보 수준 (CRITICAL, WARNING, CAUTION, ATTENTION, NORMAL) 또는 null (임계값 없음)
     */
    public static String getFloodWarningLevel(WaterLevelData data, StationThreshold threshold) {
        Double waterLevel = data.getWaterLevelValue();
        if (waterLevel == null) {
            return null;  // 수위 값이 없으면 경보 판정 불가
        }
        
        // 임계값이 없으면 경보 판정 스킵 (null 반환)
        if (threshold == null) {
            return null;
        }
        
        // 관측소별 임계값 가져오기
        Double critical = threshold.getCriticalThreshold();
        Double warning = threshold.getWarningThreshold();
        Double caution = threshold.getCautionThreshold();
        Double attention = threshold.getAttentionThreshold();
        
        // 모든 임계값이 null이면 경보 판정 스킵
        if (critical == null && warning == null && caution == null && attention == null) {
            return null;
        }
        
        // 임계값이 있는 범위만 사용하여 경보 수준 판정
        // 우선순위: CRITICAL > WARNING > CAUTION > ATTENTION
        if (critical != null && waterLevel >= critical) {
            return "CRITICAL";  // 심각
        }
        if (warning != null && waterLevel >= warning) {
            return "WARNING";   // 경계
        }
        if (caution != null && waterLevel >= caution) {
            return "CAUTION";   // 주의
        }
        if (attention != null && waterLevel >= attention) {
            return "ATTENTION"; // 관심
        }
        
        return "NORMAL";    // 정상
    }
    
    /**
     * 홍수 경보 수준 판정 (기본값 사용 - 수위만 반환)
     */
    public static String getFloodWarningLevel(WaterLevelData data) {
        return getFloodWarningLevel(data, null);
    }
    
}

