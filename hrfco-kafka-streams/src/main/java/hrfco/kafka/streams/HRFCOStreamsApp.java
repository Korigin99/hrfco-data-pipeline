// 한강 홍수통제소 수위 데이터 스트림 처리 애플리케이션
// NiFi에서 PublishKafka로 전송된 hrfco-waterlevel 토픽의 데이터 처리
// 수위 이상치 탐지, 홍수 경보 수준 분석, MinIO/TimescaleDB/MongoDB 저장
package hrfco.kafka.streams;

import hrfco.kafka.streams.cache.StationThreshold;
import hrfco.kafka.streams.cache.StationThresholdCache;
import hrfco.kafka.streams.model.WaterLevelData;
import hrfco.kafka.streams.repository.MinIORepository;
import hrfco.kafka.streams.repository.MongoDBRepository;
import hrfco.kafka.streams.repository.TimescaleDBRepository;
import hrfco.kafka.streams.service.HRFCOStorageService;
import hrfco.kafka.streams.util.StationThresholdLoader;
import hrfco.kafka.streams.validator.WaterLevelValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HRFCOStreamsApp {
    private static final Logger logger = LoggerFactory.getLogger(HRFCOStreamsApp.class);
    
    private static HRFCOStorageService hrfcoStorageService;
    private static TimescaleDBRepository timescaleDB;
    private static MongoDBRepository mongoDB;
    private static ObjectMapper objectMapper;
    private static KafkaProducer<String, String> alertProducer;
    
    public static void main(String[] args) {
        // Repository 초기화
        try {
            timescaleDB = new TimescaleDBRepository();
            MinIORepository minIO = new MinIORepository();
            mongoDB = new MongoDBRepository();
            
            // Service 초기화(Repository 주입)
            hrfcoStorageService = new HRFCOStorageService(timescaleDB, minIO, mongoDB);
            
            objectMapper = new ObjectMapper();
            logger.info("All repositories and services initialized successfully for HRFCO data processing");
            
            // 관측소 임계값 로드 (최초 실행 시에만)
            loadStationThresholds();
            
            // 알림용 Kafka Producer 초기화
            initializeAlertProducer();
            
        } catch (Exception e) {
            logger.error("Failed to initialize services", e);
            System.exit(1);
        }
        
        Properties props = new Properties();
        
        // Kafka 브로커 설정
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, 
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"));
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "hrfco-streams-processor");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        // 병렬 처리 설정 (환경 변수 또는 CPU 코어 수 기반)
        String numThreadsStr = System.getenv("NUM_STREAM_THREADS");
        int numThreads = numThreadsStr != null ? Integer.parseInt(numThreadsStr) 
                : Math.max(1, Runtime.getRuntime().availableProcessors());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThreads);
        logger.info("Kafka Streams parallel processing configured with {} threads", numThreads);
        
        // Consumer 설정
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        StreamsBuilder builder = new StreamsBuilder();
        
        // hrfco-waterlevel 토픽에서 데이터 수신
        KStream<String, String> waterLevelStream = builder.stream("hrfco-waterlevel");
        
        // 데이터 처리 스트림
        waterLevelStream.foreach((key, value) -> {
            try {
                logger.info("=== Received water level data ===");
                logger.info("Key: {}", key);
                logger.info("Value: {}", value);
                
                // JSON 파싱
                WaterLevelData waterLevelData = objectMapper.readValue(value, WaterLevelData.class);
                logger.info("Parsed water level data - ObsCode: {}, Time: {}, WL: {}, FW: {}", 
                        waterLevelData.getObservationCode(), 
                        waterLevelData.getFormattedObservationTime(),
                        waterLevelData.getWaterLevelValue(),
                        waterLevelData.getFlowRateValue());
                
                // 필수 필드 검증
                if (!WaterLevelValidator.validateRequiredFields(waterLevelData)) {
                    logger.error("Required fields validation failed for observation: {}. Skipping.", 
                            waterLevelData.getObservationCode());
                    return;
                }
                logger.info("Required fields validation passed");
                
                // 데이터 기본 범위 검증 (물리적 한계)
                boolean isRangeValid = WaterLevelValidator.validateDataRange(waterLevelData);
                
                // 관측소 임계값 조회 (캐시 우선, 없으면 MongoDB 조회)
                StationThreshold threshold = StationThresholdCache.get(
                    waterLevelData.getObservationCode()
                );
                
                if (threshold == null) {
                    // 캐시 미스: MongoDB에서 조회
                    Document stationDoc = mongoDB.getStationThreshold(
                        waterLevelData.getObservationCode()
                    );
                    
                    if (stationDoc != null) {
                        threshold = StationThreshold.fromDocument(stationDoc);
                        // 캐시에 저장
                        StationThresholdCache.put(
                            waterLevelData.getObservationCode(), 
                            threshold
                        );
                        logger.debug("Loaded threshold from DB and cached: {}", 
                            waterLevelData.getObservationCode());
                    } else {
                        logger.debug("No threshold found in DB for station: {}, using defaults", 
                            waterLevelData.getObservationCode());
                    }
                }
                
                // 이상치 판정: 기준 수위를 활용한 판정
                // 1. 기준 수위가 없는 경우 (임계값 없음) - 정상 처리하되 경보 판정 불가
                // 2. 기본 범위를 벗어난 경우 (물리적 이상치)
                // 3. 관측소별 임계값이 있는 경우, 임계값과 비교하여 판정
                boolean isAnomaly = false;
                String anomalyReason = null;
                String floodWarningLevel = null; // 경보 수준 (기준 수위가 있을 때만 설정)
                
                // 기본 범위를 벗어난 경우는 무조건 이상치 (물리적으로 불가능한 값)
                if (!isRangeValid) {
                    isAnomaly = true;
                    anomalyReason = "Data out of physical range";
                    logger.warn("Anomaly detected: {}", anomalyReason);
                } else if (threshold == null) {
                    // 기준 수위 정보가 아예 없는 경우: 정상 데이터로 처리하되 경보 판정 불가
                    isAnomaly = false;
                    floodWarningLevel = "NO_THRESHOLD"; // 경보 판정 불가 상태
                    logger.info("No threshold data available for station: {} - treating as normal data without warning level", 
                        waterLevelData.getObservationCode());
                } else {
                    // 관측소별 기준 수위와 비교하여 이상치 판정
                    Double waterLevel = waterLevelData.getWaterLevelValue();
                    if (waterLevel != null) {
                        Double attention = threshold.getAttentionThreshold();
                        Double caution = threshold.getCautionThreshold();
                        Double warning = threshold.getWarningThreshold();
                        Double critical = threshold.getCriticalThreshold();
                        
                        // 유효한 임계값 개수 확인
                        int validThresholdCount = 0;
                        if (attention != null && attention > 0) validThresholdCount++;
                        if (caution != null && caution > 0) validThresholdCount++;
                        if (warning != null && warning > 0) validThresholdCount++;
                        if (critical != null && critical > 0) validThresholdCount++;
                        
                        if (validThresholdCount == 0) {
                            // 모든 임계값이 null이거나 0 이하: 정상 처리하되 경보 판정 불가
                            isAnomaly = false;
                            floodWarningLevel = "NO_THRESHOLD";
                            logger.info("All threshold values are null or invalid for station: {} - treating as normal data without warning level", 
                                waterLevelData.getObservationCode());
                        } else if (validThresholdCount < 2) {
                            // 유효한 임계값이 1개만 있는 경우: 정상 처리하되 경보 판정 제한적
                            isAnomaly = false;
                            floodWarningLevel = "PARTIAL_THRESHOLD";
                            logger.info("Only {} valid threshold value(s) for station: {} - limited warning capability", 
                                validThresholdCount, waterLevelData.getObservationCode());
                        } else {
                            // 유효한 임계값이 2개 이상: 정상 판정 및 경보 수준 판정 가능
                            // 기준 수위가 있으면 경보 판정만 수행하고, anomaly 판정은 물리적 범위 체크에만 의존
                            isAnomaly = false;
                            logger.debug("Station has {} valid thresholds - treating as normal data with warning level assessment", 
                                validThresholdCount);
                        }
                    }
                    
                    // 기준 수위가 충분히 있는 경우에만 홍수 경보 수준 판정
                    if (!isAnomaly && floodWarningLevel == null) {
                        floodWarningLevel = WaterLevelValidator.getFloodWarningLevel(
                            waterLevelData, 
                            threshold
                        );
                        
                        if (floodWarningLevel == null) {
                            logger.info("Flood warning level: NORMAL (threshold available but no level matched)");
                            floodWarningLevel = "NORMAL";
                        } else {
                            logger.info("Flood warning level: {} (threshold: {})", 
                                floodWarningLevel, threshold.toString());
                        }
                    }
                }
                
                // 기준 수위 정보 로깅
                if (threshold != null) {
                    logger.info("Station threshold comparison - ObsCode: {}, WaterLevel: {} m, Thresholds: {}, Warning Level: {}", 
                        waterLevelData.getObservationCode(),
                        waterLevelData.getWaterLevelValue(),
                        threshold.toString(),
                        floodWarningLevel != null ? floodWarningLevel : "N/A");
                } else {
                    logger.info("No threshold data available for station: {} - treating as normal data", 
                        waterLevelData.getObservationCode());
                }
                
                logger.info("Anomaly detection result: {} (range valid: {}, warning: {}, reason: {})", 
                        isAnomaly, isRangeValid, 
                        floodWarningLevel != null ? floodWarningLevel : "N/A",
                        anomalyReason != null ? anomalyReason : "NORMAL");
                
                // 저장(Service 계층을 통한 3중 저장)
                if (isAnomaly) {
                    logger.warn("=== ANOMALY DETECTED ===");
                    logger.warn("Observation: {}, Water Level: {} m, Flow Rate: {} CMS, Warning: {}, Reason: {}", 
                            waterLevelData.getObservationCode(),
                            waterLevelData.getWaterLevelValue(), 
                            waterLevelData.getFlowRateValue(),
                            floodWarningLevel,
                            anomalyReason);
                    
                    // HRFCO Storage Service를 통해 이상치 저장
                    hrfcoStorageService.saveWaterLevelData(waterLevelData, value, true, floodWarningLevel);
                } else {
                    logger.info("=== NORMAL DATA ===");
                    logger.info("Observation: {}, Water Level: {} m, Flow Rate: {} CMS, Warning Level: {}", 
                            waterLevelData.getObservationCode(),
                            waterLevelData.getWaterLevelValue(), 
                            waterLevelData.getFlowRateValue(),
                            floodWarningLevel != null ? floodWarningLevel : "NORMAL");
                    
                    // HRFCO Storage Service를 통해 정상 저장
                    hrfcoStorageService.saveWaterLevelData(waterLevelData, value, false, floodWarningLevel);
                }
                
                // 알림 발송: 모든 데이터를 모니터링 서버로 전송
                // 1. 이상치인 경우: ANOMALY로 전송
                // 2. 정상 데이터: 경보 수준에 따라 전송
                if (isAnomaly) {
                    logger.info("Sending anomaly alert for observation: {}", waterLevelData.getObservationCode());
                    sendAlert(waterLevelData, "ANOMALY", true);
                } else {
                    // 정상 데이터: 경보 수준에 따라 전송
                    // NO_THRESHOLD: 기준 수위 정보가 없는 경우 -> NORMAL로 전송
                    // PARTIAL_THRESHOLD: 부분 임계값만 있는 경우 -> NORMAL로 전송
                    String alertLevel = floodWarningLevel;
                    if ("NO_THRESHOLD".equals(floodWarningLevel) || "PARTIAL_THRESHOLD".equals(floodWarningLevel)) {
                        alertLevel = "NORMAL";
                        logger.info("Sending normal data alert for observation: {} (threshold status: {})", 
                            waterLevelData.getObservationCode(), floodWarningLevel);
                    } else {
                        logger.info("Sending normal data alert for observation: {}, level: {}", 
                            waterLevelData.getObservationCode(), alertLevel);
                    }
                    sendAlert(waterLevelData, alertLevel, false);
                }
                
                logger.info("=== Processing completed ===");
                
            } catch (Exception e) {
                logger.error("=== ERROR processing water level data ===", e);
                logger.error("Key: {}", key);
                logger.error("Value: {}", value);
                logger.error("Exception type: {}", e.getClass().getName());
                logger.error("Exception message: {}", e.getMessage());
                e.printStackTrace();
            }
        });
        
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        
        // 종료 훅 추가
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down HRFCO Kafka Streams application...");
            streams.close();
            if (hrfcoStorageService != null) {
                hrfcoStorageService.shutdown();
            }
            if (timescaleDB != null) {
                timescaleDB.close();
            }
            if (mongoDB != null) {
                mongoDB.close();
            }
            if (alertProducer != null) {
                alertProducer.close();
            }
        }));
        
        // 스트림 시작
        logger.info("Starting HRFCO Kafka Streams application...");
        streams.start();
        
        logger.info("HRFCO Kafka Streams application is running. Processing hrfco-waterlevel topic...");
        logger.info("Features: Data validation, Flood warning detection");
        logger.info("Storage: MinIO (waterlevel/), TimescaleDB (hrfco schema), MongoDB (water_level_data collection)");
        logger.info("API Fields: wlobscd (obs code), ymdhm (time), wl (water level), fw (flow rate)");
        logger.info("Station thresholds: Loaded from API and cached in memory");
    }
    
    /**
     * 관측소 임계값 로드 (최초 실행 시에만)
     */
    private static void loadStationThresholds() {
        try {
            logger.info("Loading station thresholds from HRFCO API...");
            
            // 캐시 초기화 (이전 데이터 제거)
            StationThresholdCache.clear();
            logger.info("Station threshold cache cleared before loading");
            
            StationThresholdLoader loader = new StationThresholdLoader(mongoDB);
            loader.loadStationThresholds();
            
            logger.info("Station thresholds loading completed. Cached stations: {}", 
                StationThresholdCache.size());
        } catch (Exception e) {
            logger.error("Failed to load station thresholds", e);
            // 실패해도 계속 실행 (기본값 사용)
        }
    }
    
    /**
     * 알림용 Kafka Producer 초기화
     */
    private static void initializeAlertProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        alertProducer = new KafkaProducer<>(props);
        logger.info("Alert producer initialized");
    }
    
    /**
     * 경보 알림 발송
     */
    private static void sendAlert(WaterLevelData waterLevelData, String warningLevel, boolean isAnomaly) {
        try {
            // 알림 메시지 생성
            Map<String, Object> alert = new HashMap<>();
            alert.put("observationCode", waterLevelData.getObservationCode());
            alert.put("observationTime", waterLevelData.getFormattedObservationTime());
            alert.put("waterLevel", waterLevelData.getWaterLevelValue());
            alert.put("flowRate", waterLevelData.getFlowRateValue());
            alert.put("warningLevel", warningLevel);
            alert.put("isAnomaly", isAnomaly);
            
            String message;
            if (isAnomaly) {
                message = String.format("관측소 %s에서 이상치 감지 (수위: %.2f m)",
                    waterLevelData.getObservationCode(), waterLevelData.getWaterLevelValue());
            } else {
                message = String.format("관측소 %s에서 %s 수위 경보 (수위: %.2f m)",
                    waterLevelData.getObservationCode(), warningLevel, waterLevelData.getWaterLevelValue());
            }
            alert.put("message", message);
            alert.put("timestamp", System.currentTimeMillis());
            
            String alertJson = objectMapper.writeValueAsString(alert);
            
            // Kafka 알림 토픽으로 발송
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "hrfco-alerts",  // 알림 토픽
                waterLevelData.getObservationCode(),  // 키: 관측소 코드
                alertJson  // 값: 알림 JSON
            );
            
            alertProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to send alert for observation: {}", 
                        waterLevelData.getObservationCode(), exception);
                } else {
                    logger.info("Alert sent successfully: {} - {} (topic: {}, partition: {}, offset: {})", 
                        warningLevel, waterLevelData.getObservationCode(),
                        metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
            
        } catch (Exception e) {
            logger.error("Failed to create alert message", e);
        }
    }
}

