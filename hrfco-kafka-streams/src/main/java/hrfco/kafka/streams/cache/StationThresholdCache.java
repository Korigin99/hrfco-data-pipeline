// 관측소 임계값 메모리 캐시
package hrfco.kafka.streams.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 관측소 임계값 메모리 캐시
 * 관측소 코드를 키로 하여 임계값 데이터를 메모리에 캐시
 */
public class StationThresholdCache {
    private static final Logger logger = LoggerFactory.getLogger(StationThresholdCache.class);
    
    // 메모리 캐시 (키: 관측소 코드, 값: 임계값 데이터)
    private static final Map<String, StationThreshold> cache = new ConcurrentHashMap<>();
    
    /**
     * 캐시에서 관측소 임계값 조회
     * 
     * @param observationCode 관측소 코드
     * @return 임계값 데이터 (없으면 null)
     */
    public static StationThreshold get(String observationCode) {
        return cache.get(observationCode);
    }
    
    /**
     * 캐시에 관측소 임계값 저장
     * 
     * @param observationCode 관측소 코드
     * @param threshold 임계값 데이터
     */
    public static void put(String observationCode, StationThreshold threshold) {
        if (threshold != null) {
            cache.put(observationCode, threshold);
            logger.debug("Cached threshold for station: {}", observationCode);
        }
    }
    
    /**
     * 캐시에서 관측소 임계값 제거
     * 
     * @param observationCode 관측소 코드
     */
    public static void remove(String observationCode) {
        cache.remove(observationCode);
        logger.debug("Removed threshold from cache: {}", observationCode);
    }
    
    /**
     * 캐시 전체 초기화
     */
    public static void clear() {
        cache.clear();
        logger.info("Station threshold cache cleared");
    }
    
    /**
     * 캐시 크기 확인
     * 
     * @return 캐시에 저장된 관측소 수
     */
    public static int size() {
        return cache.size();
    }
}

