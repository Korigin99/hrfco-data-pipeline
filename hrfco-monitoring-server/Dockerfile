# HRFCO Monitoring Server Dockerfile

FROM gradle:8.5-jdk17 AS builder

WORKDIR /app

# Gradle 설정 파일 복사
COPY build.gradle /app/build.gradle
COPY settings.gradle /app/settings.gradle
COPY gradle /app/gradle
COPY gradlew /app/gradlew
COPY gradlew.bat /app/gradlew.bat

# 의존성 다운로드 (캐싱을 위해 소스 코드 복사 전에)
RUN gradle dependencies --no-daemon || true

# 소스 코드 복사
COPY src /app/src

# 빌드
RUN gradle bootJar --no-daemon

# 실행 단계
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

# 빌드된 JAR 파일 복사
COPY --from=builder /app/build/libs/hrfco-monitoring-server-1.0.0.jar /app/app.jar

# 포트 노출
EXPOSE 8080

# 실행
ENTRYPOINT ["java", "-jar", "/app/app.jar"]


