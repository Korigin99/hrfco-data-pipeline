// 관측소 임계값 로더
// API에서 관측소 임계값을 가져와서 MongoDB에 저장
package hrfco.kafka.streams.util;

import hrfco.kafka.streams.repository.MongoDBRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;

/**
 * 한강 홍수통제소 API에서 관측소 임계값을 가져와서 MongoDB에 저장
 */
public class StationThresholdLoader {
    private static final Logger logger = LoggerFactory.getLogger(StationThresholdLoader.class);
    
    // 서비스 키는 환경변수에서 가져오거나 기본값 사용
    private static final String SERVICE_KEY = System.getenv().getOrDefault(
        "HRFCO_SERVICE_KEY", 
        "7F1EF036-EB93-4721-96CB-EEDC63624041"
    );
    private static final String API_URL = "https://api.hrfco.go.kr/" + SERVICE_KEY + "/waterlevel/info.json";
    
    private final MongoDBRepository mongoDB;
    private final ObjectMapper objectMapper;
    
    public StationThresholdLoader(MongoDBRepository mongoDB) {
        this.mongoDB = mongoDB;
        this.objectMapper = new ObjectMapper();
    }
    
    /**
     * API에서 관측소 임계값을 가져와서 MongoDB에 저장
     */
    public void loadStationThresholds() {
        try {
            logger.info("Loading station thresholds from API: {}", API_URL);
            
            // API 호출
            String jsonResponse = callAPI(API_URL);
            if (jsonResponse == null || jsonResponse.isEmpty()) {
                logger.error("Failed to get response from API");
                return;
            }
            
            // JSON 파싱
            JsonNode root = objectMapper.readTree(jsonResponse);
            JsonNode content = root.get("content");
            
            if (content == null || !content.isArray()) {
                logger.error("Invalid API response format");
                return;
            }
            
            int loadedCount = 0;
            int skippedCount = 0;
            
            // 각 관측소 데이터를 MongoDB에 저장
            for (JsonNode station : content) {
                try {
                    String obsCode = station.get("wlobscd").asText();
                    
                    // Document 생성 - API 응답의 모든 필드를 그대로 저장
                    Document stationDoc = new Document();
                    
                    // 모든 필드를 순회하며 저장(임계값은 파싱, 나머지는 그대로)
                    station.fields().forEachRemaining(entry -> {
                        String key = entry.getKey();
                        JsonNode value = entry.getValue();
                        
                        // 임계값 필드는 Double로 파싱 (빈 문자열이면 null)
                        if (key.equals("attwl") || key.equals("wrnwl") || 
                            key.equals("almwl") || key.equals("srswl")) {
                            stationDoc.put(key, parseDoubleOrNull(value));
                        } else {
                            // 나머지 필드는 그대로 저장
                            if (value.isNull()) {
                                stationDoc.put(key, null);
                            } else if (value.isTextual()) {
                                String textValue = value.asText().trim();
                                // 빈 문자열도 그대로 저장(필요시 null로 변환 가능)
                                stationDoc.put(key, textValue.isEmpty() ? null : textValue);
                            } else if (value.isNumber()) {
                                stationDoc.put(key, value.asDouble());
                            } else if (value.isBoolean()) {
                                stationDoc.put(key, value.asBoolean());
                            } else {
                                // 기타 타입은 문자열로 저장
                                stationDoc.put(key, value.asText());
                            }
                        }
                    });
                    
                    // 메타데이터 추가
                    stationDoc.put("created_at", new Date());
                    
                    // MongoDB에 저장(upsert) - _id는 관측소 코드로 설정됨
                    mongoDB.upsertStationThreshold(obsCode, stationDoc);
                    loadedCount++;
                    
                } catch (Exception e) {
                    logger.warn("Failed to process station data", e);
                    skippedCount++;
                }
            }
            
            logger.info("Station thresholds loaded successfully. Loaded: {}, Skipped: {}", 
                    loadedCount, skippedCount);
            
        } catch (Exception e) {
            logger.error("Failed to load station thresholds from API", e);
            // 실패해도 계속 실행 (기본값 사용)
        }
    }
    
    /**
     * API 호출
     */
    private String callAPI(String apiUrl) {
        try {
            URL url = new URL(apiUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(10000); // 10초 타임아웃
            conn.setReadTimeout(10000);
            
            int responseCode = conn.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                logger.error("API call failed with response code: {}", responseCode);
                return null;
            }
            
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            StringBuilder response = new StringBuilder();
            String line;
            
            while ((line = in.readLine()) != null) {
                response.append(line);
            }
            in.close();
            
            return response.toString();
            
        } catch (Exception e) {
            logger.error("Failed to call API: {}", apiUrl, e);
            return null;
        }
    }
    
    /**
     * JSON 노드에서 Double 값 파싱 (빈 문자열이면 null)
     */
    private Double parseDoubleOrNull(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        
        String value = node.asText().trim();
        if (value.isEmpty() || value.equals(" ")) {
            return null;
        }
        
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            logger.debug("Failed to parse double: {}", value);
            return null;
        }
    }
}

