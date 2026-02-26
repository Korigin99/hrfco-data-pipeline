# HRFCO Monitoring Server

한강 홍수통제소(HRFCO) 실시간 모니터링 서버

## 주요 기능

- **SSE(Server-Sent Events) 실시간 알림**: 웹 클라이언트로 실시간 수위 알림 전송
- **Kafka 컨슈머**: `hrfco-alerts` 토픽에서 알림 수신
- **관측소 정보 관리**: MongoDB에서 관측소 목록 조회
- **웹 대시보드**: 실시간 수위 모니터링 웹 인터페이스

## 기술 스택

- Spring Boot 3.4.13
- Spring Kafka
- MongoDB (관측소 정보 조회)
- SSE (Server-Sent Events)
- Gradle 8.5
- Java 17

## 빌드 및 실행

### 빌드

```bash
./gradlew build
```

### 실행

```bash
./gradlew bootRun
```

### JAR 파일로 실행

```bash
./gradlew bootJar
java -jar build/libs/hrfco-monitoring-server-1.0.0.jar
```

