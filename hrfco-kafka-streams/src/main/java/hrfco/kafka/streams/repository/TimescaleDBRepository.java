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
                    updated_at TIMESTAMPTZ,
                    PRIMARY KEY (id, observation_time)
                )
                """;

            try (PreparedStatement stmt = conn.prepareStatement(createTableSQL)) {
                stmt.execute();
            }

            // 이전 버전에서 만든 테이블에는 updated_at이 없다
            try (PreparedStatement stmt = conn.prepareStatement(
                    "ALTER TABLE hrfco.water_level_data ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ")) {
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

            // 멱등 저장용 고유 인덱스 (관측소 코드 + 관측 시각)
            // - 하이퍼테이블의 고유 인덱스는 파티션 열(observation_time)을 포함해야 한다
            // - upsert의 ON CONFLICT가 이 인덱스에 의존하므로 만들지 못하면 시작을 멈춘다
            //   (이미 중복 행이 있으면 생성이 실패한다. 정리 SQL은 README 참고)
            try (PreparedStatement stmt = conn.prepareStatement(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_hrfco_obs_code_time "
                    + "ON hrfco.water_level_data (observation_code, observation_time)")) {
                stmt.execute();
            } catch (SQLException e) {
                throw new RuntimeException(
                    "Unique index (observation_code, observation_time) creation failed. "
                    + "Remove duplicate rows first (see README).", e);
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
     * 같은 (observation_code, observation_time) 행이 이미 있을 때의 처리: 덮어쓰기(DO UPDATE)
     * - 정정될 수 있는 값(수위·유량·이상치 여부·경보 단계)만 덮어쓰고, created_at(처음 저장 시각)은 두고 updated_at을 찍는다
     * - 값이 그대로면 쓰지 않는다(WHERE ... IS DISTINCT FROM). 재처리·재수집처럼 같은 값이 다시 올 때
     *   충돌 행에 잠금만 걸고 새 행 버전(MVCC dead tuple)은 만들지 않으며, updated_at이 '실제로 정정된 시각'을 뜻하게 된다.
     *   NULL끼리도 같은 값으로 보도록 <> 대신 IS DISTINCT FROM을 쓴다.
     */
    private static final String ON_CONFLICT_CLAUSE = """
        ON CONFLICT (observation_code, observation_time) DO UPDATE SET
            water_level = EXCLUDED.water_level,
            flow_rate = EXCLUDED.flow_rate,
            is_anomaly = EXCLUDED.is_anomaly,
            flood_warning_level = EXCLUDED.flood_warning_level,
            updated_at = NOW()
        WHERE (w.water_level, w.flow_rate, w.is_anomaly, w.flood_warning_level)
            IS DISTINCT FROM
              (EXCLUDED.water_level, EXCLUDED.flow_rate, EXCLUDED.is_anomaly, EXCLUDED.flood_warning_level)
        """;

    /**
     * 한강 수위 데이터 멱등 저장 (관측소 코드 + 관측 시각 기준 upsert)
     */
    public void upsertWaterLevelData(String obsCode, Timestamp obsTime,
                                      Double waterLevel, Double flowRate,
                                      boolean isAnomaly, String floodWarning) {
        String sql = """
            INSERT INTO hrfco.water_level_data AS w
            (observation_code, observation_time, water_level, flow_rate, is_anomaly, flood_warning_level)
            VALUES (?, ?, ?, ?, ?, ?)
            """ + ON_CONFLICT_CLAUSE;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, obsCode);
            stmt.setTimestamp(2, obsTime);
            stmt.setObject(3, waterLevel);
            stmt.setObject(4, flowRate);
            stmt.setBoolean(5, isAnomaly);
            stmt.setString(6, floodWarning);

            stmt.executeUpdate();
            logger.debug("Upserted water level data: {}", obsCode);
        } catch (SQLException e) {
            logger.error("Failed to upsert water level data", e);
            throw new RuntimeException("Database upsert failed", e);
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

