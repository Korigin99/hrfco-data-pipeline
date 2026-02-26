package com.magpie.hrfco_monitoring_server.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class StationService {
    
    @Value("${mongodb.host:mongodb}")
    private String mongodbHost;
    
    @Value("${mongodb.port:27017}")
    private String mongodbPort;
    
    @Value("${mongodb.db:pipeline_db}")
    private String mongodbDb;
    
    @Value("${mongodb.user:admin}")
    private String mongodbUser;
    
    @Value("${mongodb.password:adminpassword123}")
    private String mongodbPassword;
    
    /**
     * 모든 관측소 목록 조회 (간략 정보)
     */
    public List<Map<String, Object>> getAllStations() {
        List<Map<String, Object>> stations = new ArrayList<>();
        
        try {
            String connectionString = String.format("mongodb://%s:%s@%s:%s/%s?authSource=admin",
                    mongodbUser, mongodbPassword, mongodbHost, mongodbPort, mongodbDb);
            
            try (MongoClient mongoClient = MongoClients.create(connectionString)) {
                MongoDatabase database = mongoClient.getDatabase(mongodbDb);
                MongoCollection<Document> collection = database.getCollection("observation_stations");
                
                for (Document doc : collection.find()) {
                    Map<String, Object> station = new HashMap<>();
                    String obsCode = doc.getString("_id");  // _id가 관측소 코드
                    if (obsCode == null) {
                        obsCode = doc.getString("wlobscd");  // 대체 필드
                    }
                    station.put("code", obsCode);
                    station.put("name", doc.getString("obsnm"));  // 원본 필드명
                    station.put("address", doc.getString("addr"));  // 원본 필드명
                    stations.add(station);
                }
            }
            
            log.info("Loaded {} stations", stations.size());
        } catch (Exception e) {
            log.error("Failed to load stations", e);
        }
        
        return stations;
    }
    
    /**
     * 특정 관측소의 전체 제원 정보 조회 (MongoDB 원본 그대로)
     */
    public Map<String, Object> getStationDetail(String observationCode) {
        try {
            String connectionString = String.format("mongodb://%s:%s@%s:%s/%s?authSource=admin",
                    mongodbUser, mongodbPassword, mongodbHost, mongodbPort, mongodbDb);
            
            try (MongoClient mongoClient = MongoClients.create(connectionString)) {
                MongoDatabase database = mongoClient.getDatabase(mongodbDb);
                MongoCollection<Document> collection = database.getCollection("observation_stations");
                
                // _id로 조회 (_id가 관측소 코드로 설정됨)
                Document query = new Document("_id", observationCode);
                Document doc = collection.find(query).first();
                
                if (doc != null) {
                    // MongoDB Document를 Map으로 변환 (원본 그대로)
                    Map<String, Object> stationDetail = new HashMap<>();
                    doc.forEach((key, value) -> {
                        if (!key.equals("_id")) {
                            stationDetail.put(key, value);
                        } else {
                            // _id를 observationCode로 매핑
                            stationDetail.put("observationCode", value);
                        }
                    });
                    return stationDetail;
                }
            }
        } catch (Exception e) {
            log.error("Failed to load station detail for code: {}", observationCode, e);
        }
        
        return null;
    }
}



