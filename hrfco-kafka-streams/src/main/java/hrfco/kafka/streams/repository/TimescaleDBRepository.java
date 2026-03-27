// TimescaleDB 데이터베이스 레포지토리 (Repository/DAO)
// 시계열 DB CRUD 작업에 대응

package hrfco.kafka.streams.repository;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class TimescaleDBRepository {
    private static final Logger logger = LoggerFactory.getLogger(TimescaleDBRepository.class);
    
    private final HikariDataSource dataSource;
    
    // 환경 변수에서 설정 가져오기
    private static final String DB_HOST = System.getenv().getOrDefault("TIMESCALEDB_HOST", "timescaledb");
    private static final String DB_PORT = System.getenv().getOrDefault("TIMESCALEDB_PORT", "5432");
    private static final String DB_NAME = System.getenv().getOrDefault("TIMESCALEDB_DB", "pipeline_db");
    private static final String DB_USER = System.getenv().getOrDefault("TIMESCALEDB_USER", "postgres");
    private static final String DB_PASSWORD = System.getenv().getOrDefault("TIMESCALEDB_PASSWORD", "postgrespassword123");
    
    public TimescaleDBRepository() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:postgresql://%s:%s/%s", DB_HOST, DB_PORT, DB_NAME));
        config.setUsername(DB_USER);
        config.setPassword(DB_PASSWORD);
        config.setDriverClassName("org.postgresql.Driver");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30000);
        
        this.dataSource = new HikariDataSource(config);
        
        initializeDatabase();
        
        logger.info("TimescaleDB Repository initialized. Host: {}, Database: {}", DB_HOST, DB_NAME);
    }
    
    /**
     * 데이터베이스 초기화 - 스키마 및 테이블 생성
     */
    private void initializeDatabase() {
        createSchemas();
        createHRFCOTables();
    }

    /**
     * HRFCO 스키마 생성
     */
    private void createSchemas() {
        try (Connection conn = dataSource.getConnection()) {
            String[] schemas = {"hrfco"};
            
            for (String schema : schemas) {
                String sql = String.format("CREATE SCHEMA IF NOT EXISTS %s", schema);
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.execute();
                    logger.info("Schema created/verified: {}", schema);
                } catch (SQLException e) {
                    logger.error("Failed to create schema: {}", schema, e);
                    throw new RuntimeException("Schema creation failed: " + schema, e);
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to create schemas", e);
            throw new RuntimeException("Schema initialization failed", e);
        }
    }
    
    /**
     * 한강 홍수통제소 테이블 생성
     */
    private void createHRFCOTables() {
        try (Connection conn = dataSource.getConnection()) {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS hrfco.water_level_data (
                    id BIGSERIAL,
                    observation_code VARCHAR(50) NOT NULL,
                    observation_time TIMESTAMPTZ NOT NULL,
                    water_level DOUBLE PRECISION,
                    flow_rate DOUBLE PRECISION,
                    is_anomaly BOOLEAN DEFAULT FALSE,
                    flood_warning_level VARCHAR(20),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (id, observation_time)
                )
                """;
            
            try (PreparedStatement stmt = conn.prepareStatement(createTableSQL)) {
                stmt.execute();
            }
            
            // Hypertable 생성
            String hypertableSQL = """
                SELECT create_hypertable('hrfco.water_level_data', 'observation_time', if_not_exists => TRUE)
                """;
            
            try (PreparedStatement stmt = conn.prepareStatement(hypertableSQL)) {
                stmt.execute();
            } catch (SQLException e) {
                logger.debug("HRFCO hypertable: {}", e.getMessage());
            }
            
            // 인덱스 생성
            String[] indexes = {
                "CREATE INDEX IF NOT EXISTS idx_hrfco_obs_code ON hrfco.water_level_data(observation_code)",
                "CREATE INDEX IF NOT EXISTS idx_hrfco_obs_time ON hrfco.water_level_data(observation_time)",
                "CREATE INDEX IF NOT EXISTS idx_hrfco_anomaly ON hrfco.water_level_data(is_anomaly)",
                "CREATE INDEX IF NOT EXISTS idx_hrfco_warning ON hrfco.water_level_data(flood_warning_level)"
            };
            
            for (String indexSQL : indexes) {
                try (PreparedStatement stmt = conn.prepareStatement(indexSQL)) {
                    stmt.execute();
                } catch (SQLException e) {
                    logger.debug("HRFCO index: {}", e.getMessage());
                }
            }
            
            logger.info("HRFCO tables initialized in hrfco schema");
        } catch (SQLException e) {
            logger.error("Failed to create HRFCO tables", e);
            throw new RuntimeException("HRFCO table creation failed", e);
        }
    }
    
    /**
     * 한강 수위 데이터 삽입
     */
    public void insertWaterLevelData(String obsCode, Timestamp obsTime,
                                      Double waterLevel, Double flowRate,
                                      boolean isAnomaly, String floodWarning) {
        String sql = """
            INSERT INTO hrfco.water_level_data
            (observation_code, observation_time, water_level, flow_rate, is_anomaly, flood_warning_level)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, obsCode);
            stmt.setTimestamp(2, obsTime);
            stmt.setObject(3, waterLevel);
            stmt.setObject(4, flowRate);
            stmt.setBoolean(5, isAnomaly);
            stmt.setString(6, floodWarning);

            stmt.executeUpdate();
            logger.debug("Inserted water level data: {}", obsCode);
        } catch (SQLException e) {
            logger.error("Failed to insert water level data", e);
            throw new RuntimeException("Database insert failed", e);
        }
    }
    
    /**
     * 연결 풀 종료
     */
    public void close() {
        if (dataSource != null) {
            dataSource.close();
            logger.info("TimescaleDB connection pool closed");
        }
    }
    
    /**
     * DB 연결 가져오기(직접 사용)
     */
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}

