# HRFCO Kafka Streams Application

한강 홍수통제소(HRFCO) 수위 데이터 실시간 처리 애플리케이션

## 주요 기능

- **실시간 데이터 처리**: Kafka Streams를 통한 수위 데이터 스트림 처리
- **이상치 탐지**: 수위 및 유량 데이터의 범위 검증
- **홍수 경보 수준 분석**: 관측소별 임계값 기반 경보 수준 판정 (관심, 주의, 경계, 심각)
- **3중 저장**: 
  - MinIO: JSON 파일 저장 (정상/이상 분리)
  - TimescaleDB: 시계열 데이터베이스 저장
  - MongoDB: 원본 JSON 데이터 저장
- **알림 발송**: Kafka를 통한 실시간 알림 전송

## 기술 스택

- Java 17
- Kafka Streams 3.6.1
- MinIO (Object Storage)
- TimescaleDB (Time Series Database)
- MongoDB (Document Database)
- Jackson (JSON Processing)
- SLF4J + Simple Logger

## 빌드 및 실행

### 빌드

```bash
cd app
./gradlew build
```

### 실행

```bash
./gradlew run
```

### JAR 파일로 실행

```bash
./gradlew jar
java -jar app/build/libs/hrfco-kafka-streams-1.0.0.jar
```

## 환경 변수

| 변수명 | 설명 | 기본값 |
|--------|------|--------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka 브로커 주소 | `kafka:9092` |
| `MINIO_ENDPOINT` | MinIO 엔드포인트 | `minio:9000` |
| `MINIO_ACCESS_KEY` | MinIO 액세스 키 | `admin` |
| `MINIO_SECRET_KEY` | MinIO 시크릿 키 | `adminpassword123` |
| `MINIO_BUCKET` | MinIO 버킷 이름 | `iot-data` |
| `TIMESCALEDB_HOST` | TimescaleDB 호스트 | `timescaledb` |
| `TIMESCALEDB_PORT` | TimescaleDB 포트 | `5432` |
| `TIMESCALEDB_DB` | TimescaleDB 데이터베이스 | `pipeline_db` |
| `TIMESCALEDB_USER` | TimescaleDB 사용자 | `postgres` |
| `TIMESCALEDB_PASSWORD` | TimescaleDB 비밀번호 | `postgrespassword123` |
| `MONGODB_HOST` | MongoDB 호스트 | `mongodb` |
| `MONGODB_PORT` | MongoDB 포트 | `27017` |
| `MONGODB_DB` | MongoDB 데이터베이스 | `pipeline_db` |
| `MONGODB_USER` | MongoDB 사용자 | `admin` |
| `MONGODB_PASSWORD` | MongoDB 비밀번호 | `adminpassword123` |
| `HRFCO_SERVICE_KEY` | HRFCO API 서비스 키 | `7F1EF036-EB93-4721-96CB-EEDC63624041` |

## 데이터 처리 흐름

1. **Kafka 토픽 수신**: `hrfco-waterlevel` 토픽에서 수위 데이터 수신
2. **데이터 검증**: 필수 필드 및 데이터 범위 검증
3. **임계값 조회**: 관측소별 임계값 조회 (캐시 우선, 없으면 MongoDB 조회)
4. **경보 수준 판정**: 수위에 따른 홍수 경보 수준 결정
5. **이상치 탐지**: 범위를 벗어난 데이터 이상치 판정
6. **3중 저장**: MinIO, TimescaleDB, MongoDB에 동시 저장
7. **알림 전송**: 이상치 또는 높은 경보 수준 시 `hrfco-alerts` 토픽으로 알림 전송

## 데이터 구조

### 입력 데이터 (Kafka hrfco-waterlevel 토픽)

```json
{
  "wlobscd": "1001602",           // 관측소 코드
  "ymdhm": "202512091530",        // 관측 시간 (yyyyMMddHHmm)
  "wl": "1.23",                   // 수위 (m)
  "fw": "45.67"                   // 유량 (CMS)
}
```

### 출력 데이터 (Kafka hrfco-alerts 토픽)

```json
{
  "observationCode": "1001602",
  "observationTime": "2025-12-09 15:30",
  "waterLevel": 1.23,
  "flowRate": 45.67,
  "warningLevel": "NORMAL",       // NORMAL, ATTENTION, CAUTION, WARNING, CRITICAL, ANOMALY
  "isAnomaly": false,
  "message": "관측소 1001602에서 이상치 감지 (수위: 1.23 m)",
  "timestamp": 1733729400000
}
```

## 저장소 구조

### MinIO
- 경로: `waterlevel/{normal|anomalies}/yyyy/MM/dd/obs_{code}_{timestamp}.json`

### TimescaleDB
- 스키마: `hrfco`
- 테이블: `water_level_data`

### MongoDB
- 데이터베이스: `pipeline_db`
- 컬렉션: `water_level_data`, `observation_stations`

## 라이선스

Proprietary

