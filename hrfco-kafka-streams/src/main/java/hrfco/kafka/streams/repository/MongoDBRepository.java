// MongoDB 데이터베이스 레포지토리 (Repository)
// Document CRUD 작업에 대응

package hrfco.kafka.streams.repository;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDBRepository {
    private static final Logger logger = LoggerFactory.getLogger(MongoDBRepository.class);
    
    private final MongoClient mongoClient;
    private final MongoDatabase database;
    
    private static final String MONGODB_HOST = System.getenv().getOrDefault("MONGODB_HOST", "mongodb");
    private static final String MONGODB_PORT = System.getenv().getOrDefault("MONGODB_PORT", "27017");
    private static final String MONGODB_DB = System.getenv().getOrDefault("MONGODB_DB", "pipeline_db");
    private static final String MONGODB_USER = System.getenv().getOrDefault("MONGODB_USER", "admin");
    private static final String MONGODB_PASSWORD = System.getenv().getOrDefault("MONGODB_PASSWORD", "adminpassword123");
    
    public MongoDBRepository() {
        try {
            String connectionString = String.format("mongodb://%s:%s@%s:%s/%s?authSource=admin",
                    MONGODB_USER, MONGODB_PASSWORD, MONGODB_HOST, MONGODB_PORT, MONGODB_DB);
            
            this.mongoClient = MongoClients.create(connectionString);
            this.database = mongoClient.getDatabase(MONGODB_DB);
            
            logger.info("MongoDB Repository initialized. Host: {}, Database: {}", MONGODB_HOST, MONGODB_DB);
        } catch (Exception e) {
            logger.error("Failed to initialize MongoDB client", e);
            throw new RuntimeException("MongoDB initialization failed", e);
        }
    }
    
    /**
     * JSON 문자열 저장(범용)
     * 
     * @param collectionName 컬렉션 이름
     * @param jsonData JSON 문자열
     */
    public void insertDocument(String collectionName, String jsonData) {
        try {
            MongoCollection<Document> collection = database.getCollection(collectionName);
            Document document = Document.parse(jsonData);
            collection.insertOne(document);
            
            logger.debug("Inserted document to MongoDB collection: {}", collectionName);
        } catch (Exception e) {
            logger.error("Failed to insert document to MongoDB", e);
            throw new RuntimeException("MongoDB insert failed", e);
        }
    }
    
    /**
     * Document 객체로 저장
     * 
     * @param collectionName 컬렉션 이름
     * @param document Document 객체
     */
    public void insertDocument(String collectionName, Document document) {
        try {
            MongoCollection<Document> collection = database.getCollection(collectionName);
            collection.insertOne(document);
            
            logger.debug("Inserted document to MongoDB collection: {}", collectionName);
        } catch (Exception e) {
            logger.error("Failed to insert document to MongoDB", e);
            throw new RuntimeException("MongoDB insert failed", e);
        }
    }
    
    /**
     * 컬렉션 가져오기
     */
    public MongoCollection<Document> getCollection(String collectionName) {
        return database.getCollection(collectionName);
    }
    
    /**
     * 관측소 임계값 데이터 업데이트 (upsert)
     * 
     * @param observationCode 관측소 코드
     * @param stationData 관측소 임계값 데이터 Document
     */
    public void upsertStationThreshold(String observationCode, Document stationData) {
        try {
            MongoCollection<Document> collection = database.getCollection("observation_stations");
            
            // 관측소 코드를 _id로 설정
            stationData.put("_id", observationCode);
            stationData.put("updated_at", new java.util.Date());
            
            // upsert (있으면 업데이트, 없으면 삽입)
            collection.replaceOne(
                new Document("_id", observationCode),
                stationData,
                new com.mongodb.client.model.ReplaceOptions().upsert(true)
            );
            
            logger.debug("Upserted station threshold: {}", observationCode);
        } catch (Exception e) {
            logger.error("Failed to upsert station threshold", e);
            throw new RuntimeException("MongoDB upsert failed", e);
        }
    }
    
    /**
     * 관측소 임계값 데이터 조회
     * 
     * @param observationCode 관측소 코드
     * @return 관측소 임계값 데이터 Document (없으면 null)
     */
    public Document getStationThreshold(String observationCode) {
        try {
            MongoCollection<Document> collection = database.getCollection("observation_stations");
            Document result = collection.find(new Document("_id", observationCode)).first();
            
            if (result != null) {
                logger.debug("Found station threshold: {}", observationCode);
            } else {
                logger.debug("Station threshold not found: {}", observationCode);
            }
            
            return result;
        } catch (Exception e) {
            logger.error("Failed to get station threshold", e);
            return null;
        }
    }
    
    /**
     * 연결 종료
     */
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
            logger.info("MongoDB connection closed");
        }
    }
}

