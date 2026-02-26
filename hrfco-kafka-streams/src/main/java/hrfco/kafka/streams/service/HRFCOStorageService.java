// 한강 홍수통제소 수위 데이터 저장 통합 서비스
// Repository를 사용하여 HRFCO 전용 저장 로직 구현

package hrfco.kafka.streams.service;

import hrfco.kafka.streams.model.WaterLevelData;
import hrfco.kafka.streams.repository.MinIORepository;
import hrfco.kafka.streams.repository.MongoDBRepository;
import hrfco.kafka.streams.repository.TimescaleDBRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 한강 홍수통제소 수위 데이터 저장 통합 서비스
 * - MinIO: JSON 파일 저장(정상/이상 분리)
 * - TimescaleDB: 시계열 데이터 저장(hrfco.water_level_data)
 * - MongoDB: 원본 JSON 저장(water_level_data 컬렉션)
 */
public class HRFCOStorageService {
    private static final Logger logger = LoggerFactory.getLogger(HRFCOStorageService.class);
    
    private final TimescaleDBRepository timescaleDB;
    private final MinIORepository minIO;
    private final MongoDBRepository mongoDB;
    private final ObjectMapper objectMapper;
    private final ExecutorService executorService;
    
    public HRFCOStorageService(TimescaleDBRepository timescaleDB, 
                              MinIORepository minIO, 
                              MongoDBRepository mongoDB) {
        this.timescaleDB = timescaleDB;
        this.minIO = minIO;
        this.mongoDB = mongoDB;
        this.objectMapper = new ObjectMapper();
        
        // 병렬 저장을 위한 스레드 풀 (최대 3개 스레드: MinIO, TimescaleDB, MongoDB)
        int poolSize = Integer.parseInt(System.getenv().getOrDefault("STORAGE_THREAD_POOL_SIZE", "3"));
        this.executorService = Executors.newFixedThreadPool(poolSize);
        
        logger.info("HRFCO Storage Service initialized with {} parallel storage threads", poolSize);
    }
    
    /**
     * 한강 수위 데이터 저장(3중 저장) - 병렬 처리
     * 
     * @param waterLevelData 수위 데이터 객체
     * @param originalJson 원본 JSON 문자열
     * @param isAnomaly 이상치 여부
     * @param floodWarningLevel 홍수 경보 수준
     */
    public void saveWaterLevelData(WaterLevelData waterLevelData, String originalJson, 
                                   boolean isAnomaly, String floodWarningLevel) {
        logger.info("Saving HRFCO water level data - ObsCode: {}, IsAnomaly: {}, Warning: {}", 
                waterLevelData.getObservationCode(), isAnomaly, floodWarningLevel);
        
        try {
            // 병렬로 3개 저장소에 동시 저장
            CompletableFuture<Void> minIOFuture = CompletableFuture.runAsync(
                () -> saveToMinIO(waterLevelData, isAnomaly), executorService);
            
            CompletableFuture<Void> timescaleDBFuture = CompletableFuture.runAsync(
                () -> saveToTimescaleDB(waterLevelData, isAnomaly, floodWarningLevel), executorService);
            
            CompletableFuture<Void> mongoDBFuture = CompletableFuture.runAsync(
                () -> saveToMongoDB(originalJson, isAnomaly, floodWarningLevel), executorService);
            
            // 모든 저장 작업 완료 대기 (최대 30초 타임아웃)
            CompletableFuture.allOf(minIOFuture, timescaleDBFuture, mongoDBFuture)
                .get(30, TimeUnit.SECONDS);
            
            logger.info("HRFCO water level data saved successfully to all storages (parallel)");
        } catch (Exception e) {
            logger.error("Failed to save HRFCO water level data", e);
            throw new RuntimeException("HRFCO data save failed", e);
        }
    }
    
    /**
     * 리소스 정리 (애플리케이션 종료 시 호출)
     */
    public void shutdown() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("HRFCO Storage Service executor shutdown completed");
        }
    }
    
    /**
     * MinIO 저장 경로 생성 - waterlevel/{normal|anomalies}/yyyy/MM/dd/obs_{code}_{timestamp}.json
     */
    private void saveToMinIO(WaterLevelData waterLevelData, boolean isAnomaly) {
        try {
            String type = isAnomaly ? "anomalies" : "normal";
            String objectPath = buildMinIOPath(type, waterLevelData);
            String json = objectMapper.writeValueAsString(waterLevelData);
            
            minIO.saveJson(objectPath, json);
            logger.debug("Saved to MinIO: {}", objectPath);
        } catch (Exception e) {
            logger.error("Failed to save to MinIO", e);
        }
    }
    
    /**
     * TimescaleDB 저장 hrfco.water_level_data 테이블
     */
    private void saveToTimescaleDB(WaterLevelData waterLevelData, boolean isAnomaly, String floodWarning) {
        try {
            Timestamp timestamp = parseTimestamp(waterLevelData.getFormattedObservationTime());
            
            timescaleDB.insertWaterLevelData(
                waterLevelData.getObservationCode(),
                timestamp,
                waterLevelData.getWaterLevelValue(),
                waterLevelData.getFlowRateValue(),
                isAnomaly,
                floodWarning
            );
            
            logger.debug("Saved to TimescaleDB hrfco schema");
        } catch (Exception e) {
            logger.error("Failed to save to TimescaleDB", e);
        }
    }
    
    /**
     * MongoDB 저장 water_level_data 컬렉션에 원본 JSON
     */
    private void saveToMongoDB(String originalJson, boolean isAnomaly, String floodWarning) {
        try {
            Document doc = Document.parse(originalJson);
            // 메타데이터 추가
            doc.append("is_anomaly", isAnomaly);
            doc.append("flood_warning_level", floodWarning);
            doc.append("created_at", new java.util.Date());
            
            mongoDB.insertDocument("water_level_data", doc);
            logger.debug("Saved to MongoDB water_level_data collection");
        } catch (Exception e) {
            logger.error("Failed to save to MongoDB", e);
        }
    }
    
    /**
     * MinIO 경로 생성
     */
    private String buildMinIOPath(String type, WaterLevelData waterLevelData) {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
        String datePath = now.format(formatter);
        
        return String.format("waterlevel/%s/%s/obs_%s_%s.json", 
                type, datePath, waterLevelData.getObservationCode(), 
                System.currentTimeMillis());
    }
    
    /**
     * Timestamp 변환
     * getFormattedObservationTime()에서 변환된 "yyyy-MM-dd HH:mm" 형식의 문자열을
     * 한국 시간대(KST, UTC+9)로 인식하여 TIMESTAMPTZ로 변환
     * 
     * @param timestampStr "yyyy-MM-dd HH:mm" 형식의 문자열(예: "2025-12-04 15:30")
     * @return 한국 시간대로 인식된 Timestamp (TIMESTAMPTZ로 저장됨)
     */
    private Timestamp parseTimestamp(String timestampStr) {
        try {
            if (timestampStr == null) {
                return new Timestamp(System.currentTimeMillis());
            }
            
            // 문자열을 LocalDateTime으로 파싱
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
            LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
            
            // 한국 시간대(KST, UTC+9)로 인식하여 설정
            ZoneId koreaZone = ZoneId.of("Asia/Seoul");
            ZonedDateTime zonedDateTime = localDateTime.atZone(koreaZone);
            
            // Instant로 변환하여 Timestamp 생성 (TIMESTAMPTZ로 저장됨)
            return Timestamp.from(zonedDateTime.toInstant());
        } catch (Exception e) {
            logger.warn("Failed to parse timestamp: {}, using current time", timestampStr, e);
            return new Timestamp(System.currentTimeMillis());
        }
    }
}

