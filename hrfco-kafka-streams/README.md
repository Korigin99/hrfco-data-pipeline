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
- **멱등 저장**: 관측소 코드 + 관측 시각을 키로 세 저장소 모두 덮어쓰기 (재처리·재수집에도 중복 없음)
- **DLQ 재처리**: 저장 실패로 `hrfco-waterlevel-dlq`에 간 레코드를 저장소가 회복되면 자동으로 다시 저장
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
| `HRFCO_SERVICE_KEY` | HRFCO API 서비스 키 (필수, 루트 `.env`에 설정) | 없음 — `.env.example` 참고 |
| `DLQ_TOPIC` | DLQ 토픽 | `hrfco-waterlevel-dlq` |
| `DLQ_REPROCESS_ENABLED` | DLQ 재처리 컨슈머 실행 여부 (`false`면 끔) | 켜짐 |
| `DLQ_REPROCESS_BACKOFF_MS` | 재처리가 또 실패했을 때 다시 시도하기까지 대기(ms) | `10000` |

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

세 저장소 모두 **관측소 코드 + 관측 시각(yyyyMMddHHmm)**을 키로 쓴다. 같은 관측값은 몇 번 저장해도 한 건이다.

### MinIO
- 경로: `waterlevel/{normal|anomalies}/{관측일 yyyy/MM/dd}/obs_{code}_{yyyyMMddHHmm}.json` — 같은 경로에 덮어쓴다
- 정정으로 이상치 판정이 바뀌면 반대쪽 폴더의 객체를 지운다

### TimescaleDB
- 스키마: `hrfco`
- 테이블: `water_level_data`
- 고유 인덱스 `uq_hrfco_obs_code_time (observation_code, observation_time)` + `INSERT ... ON CONFLICT DO UPDATE`
  - 값이 바뀐 경우에만 덮어쓰고 `updated_at`을 찍는다. `created_at`은 처음 저장 시각 그대로

### MongoDB
- 데이터베이스: `pipeline_db`
- 컬렉션: `water_level_data`, `observation_stations`
- `water_level_data`의 `_id` = `{관측소코드}_{yyyyMMddHHmm}`, `updateOne(upsert)`. `created_at`은 처음 저장할 때만 쓴다

## DLQ 재처리

- 저장 실패(`Partial storage failure`, `All storages failed`)로 DLQ에 간 레코드만 세 저장소 모두에 다시 저장한다.
  저장이 멱등이라 이미 성공한 저장소에 다시 써도 중복이 생기지 않는다. 경보는 다시 보내지 않는다.
- 처리 실패(파싱 실패 등)는 다시 해도 같은 이유로 실패하므로 로그만 남기고 넘긴다.
- 다시 저장해도 실패하면 오프셋을 커밋하지 않고 그 레코드로 되감은 뒤 `DLQ_REPROCESS_BACKOFF_MS`만큼 기다렸다 재시도한다.
  저장소가 회복될 때까지 기다리므로 레코드를 버리지 않고 순서도 유지한다. 컨슈머 그룹은 `hrfco-dlq-reprocessor`.
- 한계: 덮어쓰기는 마지막에 쓴 값이 이긴다. 재처리가 늦게 돌면 그 사이 들어온 더 새로운 정정값을 옛 값으로 덮을 수 있다
  (HRFCO 데이터에 버전 정보가 없어 다루지 않음).

## 이전 버전 데이터가 있을 때

고유 인덱스는 이미 중복 행이 있으면 만들어지지 않고, 이때 앱은 시작하지 않는다. 키마다 가장 나중에 저장한 행만 남기고 지운 뒤 다시 시작한다.

```sql
DELETE FROM hrfco.water_level_data a
USING hrfco.water_level_data b
WHERE a.observation_code = b.observation_code
  AND a.observation_time = b.observation_time
  AND a.id < b.id;
```

MongoDB(`_id`가 ObjectId인 문서)와 MinIO(처리일·밀리초 경로)의 이전 데이터는 새 키와 겹치지 않으므로 그대로 남는다.

## 라이선스

Proprietary

