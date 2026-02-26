// MinIO 데이터베이스 레포지토리 (Repository)
// Object Storage CRUD 작업에 대응

package hrfco.kafka.streams.repository;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class MinIORepository {
    private static final Logger logger = LoggerFactory.getLogger(MinIORepository.class);
    
    private final MinioClient minioClient;
    private final String bucketName;
    
    private static final String MINIO_ENDPOINT = System.getenv().getOrDefault("MINIO_ENDPOINT", "minio:9000");
    private static final String MINIO_ACCESS_KEY = System.getenv().getOrDefault("MINIO_ACCESS_KEY", "admin");
    private static final String MINIO_SECRET_KEY = System.getenv().getOrDefault("MINIO_SECRET_KEY", "adminpassword123");
    private static final String MINIO_BUCKET = System.getenv().getOrDefault("MINIO_BUCKET", "iot-data");
    
    public MinIORepository() {
        try {
            String[] endpointParts = MINIO_ENDPOINT.split(":");
            String host = endpointParts[0];
            int port = endpointParts.length > 1 ? Integer.parseInt(endpointParts[1]) : 9000;
            
            this.minioClient = MinioClient.builder()
                    .endpoint(host, port, false)
                    .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
                    .build();
            
            this.bucketName = MINIO_BUCKET;
            
            ensureBucketExists();
            
            logger.info("MinIO Repository initialized. Endpoint: {}, Bucket: {}", MINIO_ENDPOINT, bucketName);
        } catch (Exception e) {
            logger.error("Failed to initialize MinIO client", e);
            throw new RuntimeException("MinIO initialization failed", e);
        }
    }
    
    /**
     * 버킷 존재 확인 및 생성
     */
    private void ensureBucketExists() {
        try {
            boolean found = minioClient.bucketExists(BucketExistsArgs.builder()
                    .bucket(bucketName)
                    .build());
            
            if (!found) {
                minioClient.makeBucket(MakeBucketArgs.builder()
                        .bucket(bucketName)
                        .build());
                logger.info("Created bucket: {}", bucketName);
            }
        } catch (Exception e) {
            logger.error("Error checking/creating bucket", e);
            throw new RuntimeException("Bucket operation failed", e);
        }
    }
    
    /**
     * JSON 파일 저장(범용)
     * 
     * @param objectPath 저장 경로 (예: "iot/normal/2025/12/04/sensor_001.json")
     * @param jsonContent JSON 문자열
     */
    public void saveJson(String objectPath, String jsonContent) {
        try {
            byte[] contentBytes = jsonContent.getBytes(StandardCharsets.UTF_8);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(contentBytes);
            
            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectPath)
                    .stream(inputStream, contentBytes.length, -1)
                    .contentType("application/json")
                    .build());
            
            logger.debug("Saved JSON to MinIO: {}", objectPath);
        } catch (Exception e) {
            logger.error("Failed to save JSON to MinIO: {}", objectPath, e);
            throw new RuntimeException("MinIO save failed", e);
        }
    }
    
    /**
     * 버킷 이름 가져오기
     */
    public String getBucketName() {
        return bucketName;
    }
}

