# HRFCO Data Pipeline

한강홍수통제소(HRFCO) 관측소 제원 및 관측값을 수집하고, 실시간으로 검증/분석하여 이상치 및 홍수 경보 신호를 생성하며, NiFi 파이프라인 상태까지 모니터링하는 데이터 파이프라인 프로젝트입니다.

## 프로젝트 개요

이 프로젝트는 다음 목적을 위해 설계되었습니다.

- 공공 수문 데이터(HRFCO)를 안정적으로 수집
- 수위/유량 데이터 품질 검증 및 이상치 탐지
- 관측소별 임계값 기반 경보 레벨 산정
- 다중 저장소(MinIO, TimescaleDB, MongoDB) 동시 적재
- 운영 대시보드에서 실시간 알림(SSE)과 NiFi 상태 관측

즉, 단순 수집기가 아니라 **수집 + 스트림 분석 + 저장 + 운영 모니터링**을 하나의 스택으로 통합한 구조입니다.

## 핵심 아키텍처

```text
HRFCO Open API
   -> Apache NiFi (수집/라우팅)
   -> Kafka Topic: hrfco-waterlevel
   -> hrfco-kafka-streams
      -> 필수 필드/범위 검증
      -> 이상치 판정
      -> 관측소 임계값 기반 경보 레벨 산정
      -> MinIO + TimescaleDB + MongoDB 병렬 저장
      -> Kafka Topic: hrfco-alerts
   -> hrfco-monitoring-server
      -> SSE 실시간 알림 스트림
      -> 관측소 정보 조회
      -> NiFi API 연동 메트릭/상태 모니터링
      -> Web Dashboard (/test.html)
```

## 주요 기능

### 1) 데이터 수집 (NiFi)

- HRFCO API 호출 및 데이터 수집 플로우 운영
- 수집 데이터를 Kafka로 발행하여 처리 계층과 분리
- 운영 중 큐/백프레셔/프로세서 상태를 API로 관찰 가능

### 2) 실시간 분석 (`hrfco-kafka-streams`)

- 입력 토픽: `hrfco-waterlevel`
- 필수 필드 검증: 관측소 코드, 관측시각, 수위/유량 최소 1개
- 물리 범위 검증:
  - 수위: `-10.0 ~ 50.0`
  - 유량: `0.0 ~ 50000.0`
- 관측소 임계값(`attwl`, `wrnwl`, `almwl`, `srswl`) 기반 경보 수준 산정:
  - `NORMAL`, `ATTENTION`, `CAUTION`, `WARNING`, `CRITICAL`
- 임계값 부재/부분 임계값 케이스 처리:
  - `NO_THRESHOLD`, `PARTIAL_THRESHOLD` 내부 상태로 관리 후 알림은 `NORMAL`로 표준화
- 이상치 데이터는 `ANOMALY`로 별도 알림 발행

### 3) 저장 전략 (3중 저장)

- **MinIO**: JSON 파일 아카이브 (`waterlevel/normal|anomalies/...`)
- **TimescaleDB**: 시계열 분석용 정형 저장 (`hrfco.water_level_data`)
- **MongoDB**: 원본 JSON + 메타데이터 저장 (`water_level_data`, `observation_stations`)

`hrfco-kafka-streams`는 저장 작업을 병렬 실행하여 처리량과 지연시간을 균형 있게 관리합니다.

### 4) 모니터링 서버 (`hrfco-monitoring-server`)

- Kafka `hrfco-alerts` 수신 후 SSE로 브라우저 푸시
- 관측소 목록/상세 조회 API 제공
- NiFi 상태(Online/Offline) 및 상세 메트릭 API 제공
- 대시보드(`test.html`)에서 실시간 알림/시스템 상태 가시화

## 기술 스택

- Java 17
- Spring Boot 3.4.x
- Apache NiFi 2.6.0
- Apache Kafka 4.1.0 (KRaft)
- Kafka Streams 3.6.1
- TimescaleDB (PostgreSQL 16 계열)
- MongoDB 7.0
- MinIO
- Docker / Docker Compose

## 프로젝트 구조

```text
hrfco-data-pipeline/
|- docker-compose.yml
|- README.md
|- hrfco-kafka-streams/
|  |- src/main/java/hrfco/kafka/streams/
|  |- Dockerfile
|  `- README.md
`- hrfco-monitoring-server/
   |- src/main/java/com/magpie/hrfco_monitoring_server/
   |- src/main/resources/static/test.html
   |- Dockerfile
   `- README.md
```

## 빠른 시작 (Windows / PowerShell)

### 1) 사전 요구사항

- Docker Desktop (WSL2 백엔드 권장)
- 최소 8GB RAM (16GB 권장)

### 2) 실행

```powershell
# 저장소 루트 디렉터리로 이동
Set-Location .\hrfco-data-pipeline
docker compose up -d
docker compose ps
```

초기 실행 시 이미지 다운로드로 수 분 소요될 수 있습니다.

### 3) 접속 정보

| 구성요소 | 주소 | 비고 |
|---|---|---|
| Monitoring Dashboard | `http://localhost:8081/test.html` | 실시간 대시보드 |
| NiFi UI | `https://localhost:8443/nifi/` | `admin / AdminPassword123456` |
| MinIO Console | `http://localhost:9001` | `admin / adminpassword123` |
| TimescaleDB | `localhost:5433` | `postgres / postgrespassword123` |
| MongoDB | `localhost:27017` | `admin / adminpassword123` |
| Kafka Broker | `localhost:9092` | 단일 브로커 |

> 참고: TimescaleDB는 로컬 PostgreSQL 충돌 방지를 위해 `5433` 포트를 사용합니다.

## 주요 API (Monitoring Server)

기본 주소: `http://localhost:8081`

- `GET /api/alerts/stream` : 전체 관측소 SSE 스트림
- `GET /api/alerts/stream?station={code}` : 특정 관측소 SSE 스트림
- `GET /api/alerts/stations` : 관측소 목록
- `GET /api/alerts/station/{observationCode}` : 관측소 상세 제원
- `GET /api/alerts/health` : 헬스 체크
- `GET /api/alerts/system-status` : Alert/Kafka/Streams/NiFi 종합 상태
- `GET /api/alerts/nifi-metrics` : NiFi 상세 메트릭
- `GET /api/alerts/nifi-api?endpoint=/flow/status` : NiFi API 프록시 테스트

PowerShell 예시:

```powershell
curl.exe -N "http://localhost:8081/api/alerts/stream"
curl.exe "http://localhost:8081/api/alerts/stations"
curl.exe "http://localhost:8081/api/alerts/nifi-metrics"
```

## 운영 명령어

```powershell
# 로그 확인
docker compose logs -f hrfco-kafka-streams
docker compose logs -f hrfco-monitoring-server
docker compose logs -f nifi

# 특정 서비스 재시작
docker compose restart hrfco-kafka-streams

# 중지
docker compose stop

# 컨테이너 제거 (데이터 볼륨 유지)
docker compose down

# 데이터까지 초기화
docker compose down -v
```

