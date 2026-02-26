package com.magpie.hrfco_monitoring_server.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.net.ssl.SSLContext;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * NiFi 모니터링 서비스
 * 
 * 주요 기능:
 * - 시스템 리소스 모니터링 (CPU, 메모리, GC 등)
 * - 프로세서 상태 및 처리량 추적
 * - 큐 상태 및 백프레셔 감지
 * - 알림/경고 메시지 수집
 */
@Service
@Slf4j
public class NiFiMonitoringService {

    // API Endpoints
    private static final String ACCESS_TOKEN_ENDPOINT = "/access/token";
    private static final String SYSTEM_DIAGNOSTICS_ENDPOINT = "/system-diagnostics";
    private static final String PROCESS_GROUP_STATUS_ENDPOINT = "/flow/process-groups/root/status";
    private static final String BULLETINS_ENDPOINT = "/flow/bulletin-board";
    
    // HTTP Headers
    private static final String HOST_HEADER_VALUE = "localhost:8443";
    private static final String CONTENT_TYPE_FORM_URLENCODED = "application/x-www-form-urlencoded";
    private static final String BEARER_PREFIX = "Bearer ";
    
    // Retry settings
    private static final int MAX_LOGIN_RETRIES = 2;
    private static final int RETRY_DELAY_MS = 500;

    @Value("${nifi.api.url:https://nifi:8443/nifi-api}")
    private String nifiApiUrl;

    @Value("${nifi.username:admin}")
    private String nifiUsername;

    @Value("${nifi.password:AdminPassword123456}")
    private String nifiPassword;

    private final ObjectMapper objectMapper;
    private final CloseableHttpClient httpClient;
    private String accessToken;
    
    // 마지막 처리 시간 추적
    private volatile Instant lastProcessingTime = null;
    
    // 이전 플로우 메트릭 저장 (변화 감지용)
    private volatile Map<String, Long> previousFlowMetrics = new HashMap<>();
    
    // 프로세서별 마지막 활동 시간 저장 (영구 저장을 위해 사용)
    private volatile Map<String, Instant> processorLastActivityTimes = new HashMap<>();

    public NiFiMonitoringService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.httpClient = createHttpClient();
    }

    /**
     * NiFi 2.0+ 인증 우회를 위한 HttpClient 생성
     */
    private CloseableHttpClient createHttpClient() {
        try {
            SSLContext sslContext = new SSLContextBuilder()
                    .loadTrustMaterial(null, (chain, authType) -> true)
                    .build();

            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
                    sslContext,
                    NoopHostnameVerifier.INSTANCE
            );

            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("https", sslsf)
                    .build();

            PoolingHttpClientConnectionManager connectionManager = 
                    new PoolingHttpClientConnectionManager(socketFactoryRegistry);

            return HttpClients.custom()
                    .setConnectionManager(connectionManager)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create HttpClient", e);
        }
    }

    /**
     * NiFi 액세스 토큰 획득
     */
    private String getAccessToken() {
        if (this.accessToken != null) {
            return this.accessToken;
        }

        for (int retryCount = 0; retryCount < MAX_LOGIN_RETRIES; retryCount++) {
            try {
                String token = attemptLogin();
                if (token != null) {
                    this.accessToken = token;
                    log.info("Successfully obtained NiFi access token");
                    return token;
                }
            } catch (Exception e) {
                log.warn("Error connecting to NiFi (attempt {}): {}", retryCount + 1, e.getMessage());
            }

            if (retryCount < MAX_LOGIN_RETRIES - 1) {
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        log.error("Failed to obtain access token after {} attempts", MAX_LOGIN_RETRIES);
        return null;
    }

    private String attemptLogin() throws Exception {
        String loginUrl = nifiApiUrl + ACCESS_TOKEN_ENDPOINT;
        log.debug("Attempting to login to NiFi: {}", loginUrl);

        HttpPost request = createLoginRequest(loginUrl);
        HttpResponse response = httpClient.execute(request);
        
        int statusCode = response.getStatusLine().getStatusCode();
        String responseBody = EntityUtils.toString(response.getEntity());

        if (statusCode >= 200 && statusCode < 300) {
            return responseBody.trim();
        } else {
            log.warn("Login failed with status {}: {}", statusCode, responseBody);
            return null;
        }
    }

    private HttpPost createLoginRequest(String url) {
        HttpPost request = new HttpPost(url);
        request.setHeader(HttpHeaders.CONTENT_TYPE, CONTENT_TYPE_FORM_URLENCODED);
        request.setHeader(HttpHeaders.HOST, HOST_HEADER_VALUE);
        
        String formData = String.format("username=%s&password=%s", nifiUsername, nifiPassword);
        request.setEntity(new StringEntity(formData, StandardCharsets.UTF_8));
        
        return request;
    }

    /**
     * NiFi API GET 호출
     */
    public JsonNode callNiFiApi(String endpoint) {
        try {
            String token = getAccessToken();
            if (token == null) {
                log.error("Cannot call NiFi API: Access token is null");
                return null;
            }

            String apiUrl = nifiApiUrl + endpoint;
            HttpGet request = new HttpGet(apiUrl);
            request.setHeader(HttpHeaders.AUTHORIZATION, BEARER_PREFIX + token);
            request.setHeader(HttpHeaders.HOST, HOST_HEADER_VALUE);
            
            log.debug("Calling NiFi API: {}", endpoint);
            HttpResponse response = httpClient.execute(request);
            
            return handleApiResponse(response, endpoint);
        } catch (Exception e) {
            log.error("Error calling NiFi API: {}", endpoint, e);
            return null;
        }
    }


    private JsonNode handleApiResponse(HttpResponse response, String endpoint) throws Exception {
        int statusCode = response.getStatusLine().getStatusCode();
        String responseBody = EntityUtils.toString(response.getEntity());

        if (statusCode == 401) {
            log.warn("Token expired, retrying login...");
            this.accessToken = null;
            return callNiFiApi(endpoint);
        }

        if (statusCode >= 200 && statusCode < 300) {
            return objectMapper.readTree(responseBody);
        } else {
            log.debug("API call returned non-success: {} - Status: {}", endpoint, statusCode);
            return null;
        }
    }

    /**
     * NiFi 상태 조회 (간단)
     */
    public Map<String, Object> getNiFiStatus() {
        Map<String, Object> result = new HashMap<>();
        try {
            JsonNode diagnostics = callNiFiApi(SYSTEM_DIAGNOSTICS_ENDPOINT);
            if (diagnostics != null) {
                result.put("status", "ONLINE");
                result.put("data", diagnostics);
            } else {
                result.put("status", "OFFLINE");
            }
        } catch (Exception e) {
            result.put("status", "ERROR");
            log.error("Failed to get NiFi status", e);
        }
        return result;
    }
    

    /**
     * NiFi 상세 메트릭 수집
     */
    public Map<String, Object> getNiFiMetrics() {
        Map<String, Object> result = new HashMap<>();
        Instant fetchStart = Instant.now();
        
        try {
            // 1. 시스템 진단 정보 조회
            JsonNode diagnostics = callNiFiApi(SYSTEM_DIAGNOSTICS_ENDPOINT);
            if (diagnostics == null) {
                result.put("status", "OFFLINE");
                return result;
            }

            result.put("status", "ONLINE");
            result.put("fetchTime", NiFiMetricsUtils.DATETIME_FORMATTER.format(fetchStart));
            
            // 2. 시스템 메트릭 추출
            extractSystemMetrics(diagnostics, result);
            
            // 3. 플로우 상태 조회
            extractFlowStatus(result);
            
            // 4. 프로세서별 메트릭 추출
            extractProcessorMetrics(result);
            
            // 5. 알림/경고 추출
            extractBulletins(result);
            
            // 6. 커넥션/큐 상태 추출
            extractConnectionStatus(result);
            
            // 7. 처리량 계산 및 변화 감지
            calculateThroughputAndChanges(result);
            
            // 8. 요약 메트릭 생성
            generateSummaryMetrics(result);
            
        } catch (Exception e) {
            result.put("status", "ERROR");
            result.put("errorMessage", e.getMessage());
            log.error("Failed to collect NiFi metrics", e);
        }
        
        return result;
    }

    /**
     * 시스템 메트릭 추출
     */
    private void extractSystemMetrics(JsonNode diagnostics, Map<String, Object> result) {
        JsonNode aggregate = diagnostics.path("systemDiagnostics").path("aggregateSnapshot");
        
        Map<String, Object> system = new HashMap<>();
        
        // CPU 및 스레드 정보
        system.put("cpuLoadAverage", aggregate.path("processorLoadAverage").asDouble());
        system.put("totalThreads", aggregate.path("totalThreads").asInt());
        system.put("availableProcessors", aggregate.path("availableProcessors").asInt());
        
        // 힙 메모리 정보
        long usedHeap = parseMemoryValue(aggregate, "usedHeap", "usedHeapBytes");
        long maxHeap = parseMemoryValue(aggregate, "maxHeap", "maxHeapBytes");
        
        system.put("usedHeap", usedHeap);
        system.put("usedHeapFormatted", NiFiMetricsUtils.formatBytes(usedHeap));
        system.put("maxHeap", maxHeap);
        system.put("maxHeapFormatted", NiFiMetricsUtils.formatBytes(maxHeap));
        
        // 힙 메모리 사용률 계산
        double heapUsagePercent = maxHeap > 0 ? ((double) usedHeap / maxHeap) * 100.0 : 0.0;
        system.put("heapUsagePercent", Math.round(heapUsagePercent * 10.0) / 10.0);
        
        // 저장소 정보
        JsonNode contentRepo = aggregate.path("contentRepositoryStorageUsage");
        if (contentRepo.isArray() && contentRepo.size() > 0) {
            JsonNode repo = contentRepo.get(0);
            system.put("contentRepoUsedSpace", repo.path("usedSpace").asText());
            system.put("contentRepoFreeSpace", repo.path("freeSpace").asText());
            system.put("contentRepoUsagePercent", repo.path("utilization").asText());
        }
        
        // 업타임
        system.put("uptime", aggregate.path("uptime").asText());
        
        result.put("system", system);
    }
    
    /**
     * 메모리 값 파싱 헬퍼 메서드
     */
    private long parseMemoryValue(JsonNode node, String numberField, String formattedField) {
        long value = node.path(numberField).asLong();
        if (value == 0) {
            JsonNode formattedNode = node.path(formattedField);
            if (formattedNode.isNumber()) {
                value = formattedNode.asLong();
            } else {
                value = NiFiMetricsUtils.parseFormattedBytes(formattedNode.asText(""));
            }
        }
        return value;
    }

    /**
     * 플로우 상태 조회
     */
    private void extractFlowStatus(Map<String, Object> result) {
        Map<String, Object> flow = new HashMap<>();
        
        // 프로세스 그룹 상태 조회
        JsonNode processGroup = callNiFiApi(PROCESS_GROUP_STATUS_ENDPOINT);
        if (processGroup != null) {
            JsonNode aggregateSnapshot = processGroup.path("processGroupStatus").path("aggregateSnapshot");
            
            // 스레드 및 큐 정보
            flow.put("activeThreadCount", aggregateSnapshot.path("activeThreadCount").asInt());
            flow.put("queuedCount", aggregateSnapshot.path("queuedCount").asInt());
            flow.put("queued", aggregateSnapshot.path("queuedCount").asText());
            
            // 프로세서 카운트
            int runningCount = 0;
            int stoppedCount = 0;
            JsonNode processorSnapshots = aggregateSnapshot.path("processorStatusSnapshots");
            if (processorSnapshots.isArray()) {
                for (JsonNode proc : processorSnapshots) {
                    String runStatus = proc.path("processorStatusSnapshot").path("runStatus").asText();
                    if ("Running".equalsIgnoreCase(runStatus)) {
                        runningCount++;
                    } else {
                        stoppedCount++;
                    }
                }
            }
            flow.put("runningCount", runningCount);
            flow.put("stoppedCount", stoppedCount);
            
            // 입출력 정보
            flow.put("flowFilesIn", aggregateSnapshot.path("flowFilesIn").asInt());
            flow.put("flowFilesOut", aggregateSnapshot.path("flowFilesOut").asInt());
            flow.put("bytesIn", aggregateSnapshot.path("bytesIn").asLong());
            flow.put("bytesOut", aggregateSnapshot.path("bytesOut").asLong());
        }
        
        result.put("flow", flow);
    }

    /**
     * 프로세서별 메트릭 추출 (status API 활용)
     */
    private void extractProcessorMetrics(Map<String, Object> result) {
        Map<String, Map<String, Object>> processors = new LinkedHashMap<>();
        Map<String, List<Map<String, Object>>> processorsByGroup = new LinkedHashMap<>();
        
        try {
            // 루트 그룹부터 시작
            extractProcessorsWithStatus("root", "Root", processors, processorsByGroup);
        } catch (Exception e) {
            log.warn("Failed to extract processor metrics", e);
        }
        
        // 기존 구조 호환성 유지
        result.put("processors", processors);
        // 그룹별 구조 추가
        result.put("processorsByGroup", processorsByGroup);
    }
    
    /**
     * 그룹별로 프로세서 status 추출
     */
    private void extractProcessorsWithStatus(String groupId, String groupName, 
                                             Map<String, Map<String, Object>> processors,
                                             Map<String, List<Map<String, Object>>> processorsByGroup) {
        try {
            // status API로 실제 데이터 가져오기
            JsonNode statusResponse = callNiFiApi("/flow/process-groups/" + groupId + "/status");
            if (statusResponse == null) {
                return;
            }
            
            JsonNode aggregateSnapshot = statusResponse.path("processGroupStatus").path("aggregateSnapshot");
            
            // 현재 그룹의 프로세서 추출
            JsonNode processorSnapshots = aggregateSnapshot.path("processorStatusSnapshots");
            if (processorSnapshots.isArray() && processorSnapshots.size() > 0) {
                List<Map<String, Object>> groupProcessors = new ArrayList<>();
                
                for (JsonNode procSnapshot : processorSnapshots) {
                    JsonNode snapshot = procSnapshot.path("processorStatusSnapshot");
                    String procId = procSnapshot.path("id").asText();
                    Map<String, Object> processorInfo = extractProcessorInfo(snapshot, procId, groupId);
                    
                    if (processorInfo != null) {
                        String procName = (String) processorInfo.get("name");
                        
                        processorInfo.put("processorId", procId);
                        processorInfo.put("groupId", groupId);
                        processorInfo.put("groupName", groupName);
                        
                        // 기존 구조: 고유 키로 저장
                        String uniqueKey = groupName.equals("Root") ? procName : groupName + "/" + procName;
                        processors.put(uniqueKey, processorInfo);
                        
                        // 그룹별 구조
                        groupProcessors.add(processorInfo);
                    }
                }
                
                if (!groupProcessors.isEmpty()) {
                    processorsByGroup.put(groupName, groupProcessors);
                }
            }
            
            // 하위 그룹 처리
            JsonNode childGroupSnapshots = aggregateSnapshot.path("processGroupStatusSnapshots");
            if (childGroupSnapshots.isArray()) {
                for (JsonNode childGroupSnapshot : childGroupSnapshots) {
                    String childGroupId = childGroupSnapshot.path("id").asText();
                    String childGroupName = childGroupSnapshot.path("processGroupStatusSnapshot").path("name").asText();
                    
                    if (!childGroupId.isEmpty()) {
                        extractProcessorsWithStatus(childGroupId, childGroupName, processors, processorsByGroup);
                    }
                }
            }
            
        } catch (Exception e) {
            log.debug("Failed to extract processors from group {}: {}", groupId, e.getMessage());
        }
    }
    
    /**
     * 프로세서 정보 추출 (평균 처리 시간, 마지막 작동 시간 포함)
     */
    private Map<String, Object> extractProcessorInfo(JsonNode snapshot, String processorId, String groupId) {
        Map<String, Object> processorInfo = new LinkedHashMap<>();
        
        String name = snapshot.path("name").asText();
        if (name.isEmpty()) {
            return null;
        }
        
        // 기본 정보
        processorInfo.put("name", name);
        processorInfo.put("type", snapshot.path("type").asText());
        processorInfo.put("runStatus", snapshot.path("runStatus").asText());
        processorInfo.put("activeThreadCount", snapshot.path("activeThreadCount").asInt());
        
        // 입출력 정보
        long flowFilesIn = snapshot.path("flowFilesIn").asLong();
        long flowFilesOut = snapshot.path("flowFilesOut").asLong();
        
        processorInfo.put("flowFilesIn", flowFilesIn);
        processorInfo.put("flowFilesOut", flowFilesOut);
        processorInfo.put("input", snapshot.path("input").asText());
        processorInfo.put("output", snapshot.path("output").asText());
        
        // 태스크 정보 및 처리 시간 계산
        int taskCount = snapshot.path("taskCount").asInt();
        long tasksDurationNanos = snapshot.path("tasksDurationNanos").asLong();
        
        processorInfo.put("taskCount", taskCount);
        
        // 1. 총 작업 시간 (모든 task를 실행하는데 걸린 전체 시간, 밀리초)
        long totalProcessingTimeMs = tasksDurationNanos / 1_000_000;
        processorInfo.put("totalProcessingTimeMs", totalProcessingTimeMs);
        
        // 2. FlowFile 1개당 평균 처리 시간 (밀리초)
        long avgProcessingTimeMs = 0;
        long flowFilesProcessed = Math.max(flowFilesIn, flowFilesOut);
        if (flowFilesProcessed > 0 && tasksDurationNanos > 0) {
            avgProcessingTimeMs = tasksDurationNanos / flowFilesProcessed / 1_000_000;
        }
        processorInfo.put("avgProcessingTimeMs", avgProcessingTimeMs);
        
        // 활동 상태 감지
        boolean hasActivity = flowFilesIn > 0 || flowFilesOut > 0 || taskCount > 0;
        processorInfo.put("hasActivity", hasActivity);
        
        // 변화 감지 및 마지막 작동 시간 (processorId 기반으로 개선)
        String previousKey = "processor_" + processorId;
        Long prevFlowFilesIn = previousFlowMetrics.get(previousKey + "_in");
        Long prevFlowFilesOut = previousFlowMetrics.get(previousKey + "_out");
        Long prevTaskCount = previousFlowMetrics.get(previousKey + "_tasks");
        Long prevTimestamp = previousFlowMetrics.get(previousKey + "_time");
        
        boolean isActive = false;
        Instant lastActivityTime = null;
        
        // 저장된 마지막 활동 시간 먼저 확인
        Instant savedLastActivity = processorLastActivityTimes.get(processorId);
        if (savedLastActivity != null) {
            lastActivityTime = savedLastActivity;
        }
        
        // 변화 감지: FlowFiles 또는 TaskCount가 증가했는지 확인
        if (prevFlowFilesIn != null && prevFlowFilesOut != null && prevTaskCount != null) {
            isActive = flowFilesIn > prevFlowFilesIn || 
                      flowFilesOut > prevFlowFilesOut || 
                      taskCount > prevTaskCount;
            
            if (isActive) {
                // 활동이 감지되면 현재 시간으로 업데이트
                lastActivityTime = Instant.now();
                previousFlowMetrics.put(previousKey + "_time", lastActivityTime.toEpochMilli());
                processorLastActivityTimes.put(processorId, lastActivityTime);
                lastProcessingTime = lastActivityTime;
            } else if (prevTimestamp != null) {
                // 활동이 없지만 이전 타임스탬프가 있으면 유지
                lastActivityTime = Instant.ofEpochMilli(prevTimestamp);
            }
        } else {
            // 첫 실행: 이전 값이 없으면
            if (hasActivity) {
                isActive = true;
                
                // 저장된 마지막 활동 시간이 없으면 현재 시간으로 설정
                if (lastActivityTime == null) {
                    lastActivityTime = Instant.now();
                    processorLastActivityTimes.put(processorId, lastActivityTime);
                    previousFlowMetrics.put(previousKey + "_time", lastActivityTime.toEpochMilli());
                    lastProcessingTime = lastActivityTime;
                }
            } else {
                // 활동이 없으면 Status History API로 추적 시도 (한 번만)
                if (lastActivityTime == null && !previousFlowMetrics.containsKey(previousKey + "_history_checked")) {
                    Instant historyTime = getLastActivityTimeFromHistory(processorId, groupId);
                    if (historyTime != null) {
                        lastActivityTime = historyTime;
                        processorLastActivityTimes.put(processorId, historyTime);
                        previousFlowMetrics.put(previousKey + "_time", historyTime.toEpochMilli());
                    }
                    // History API 체크 완료 표시
                    previousFlowMetrics.put(previousKey + "_history_checked", 1L);
                }
            }
        }
        
        // 활동 시간이 업데이트되면 저장
        if (lastActivityTime != null) {
            processorLastActivityTimes.put(processorId, lastActivityTime);
        }
        
        processorInfo.put("isActive", isActive);
        
        // 마지막 작동 시간
        if (lastActivityTime != null) {
            processorInfo.put("lastActivityTime", NiFiMetricsUtils.DATETIME_FORMATTER.format(lastActivityTime));
            Duration timeSince = Duration.between(lastActivityTime, Instant.now());
            processorInfo.put("timeSinceLastActivity", NiFiMetricsUtils.formatDuration(timeSince));
        } else {
            processorInfo.put("lastActivityTime", "N/A");
            processorInfo.put("timeSinceLastActivity", "데이터 없음");
        }
        
        // 현재 값 저장
        previousFlowMetrics.put(previousKey + "_in", flowFilesIn);
        previousFlowMetrics.put(previousKey + "_out", flowFilesOut);
        previousFlowMetrics.put(previousKey + "_tasks", (long) taskCount);
        
        return processorInfo;
    }


    /**
     * 알림/경고 추출
     */
    private void extractBulletins(Map<String, Object> result) {
        try {
            JsonNode bulletins = callNiFiApi(BULLETINS_ENDPOINT);
            if (bulletins == null) {
                result.put("bulletins", Collections.emptyList());
                result.put("bulletinErrorCount", 0);
                result.put("bulletinWarnCount", 0);
                return;
            }
            
            JsonNode bulletinsArray = bulletins.path("bulletinBoard").path("bulletins");
            
            if (!bulletinsArray.isArray() || bulletinsArray.size() == 0) {
                result.put("bulletins", Collections.emptyList());
                result.put("bulletinErrorCount", 0);
                result.put("bulletinWarnCount", 0);
                return;
            }
            
            List<Map<String, Object>> recentBulletins = new ArrayList<>();
            int errorCount = 0;
            int warnCount = 0;
            
            for (JsonNode bulletinNode : bulletinsArray) {
                JsonNode bulletin = bulletinNode.path("bulletin");
                if (bulletin.isMissingNode()) {
                    bulletin = bulletinNode;
                }
                
                String level = bulletin.path("level").asText();
                if ("ERROR".equalsIgnoreCase(level)) {
                    errorCount++;
                } else if ("WARNING".equalsIgnoreCase(level) || "WARN".equalsIgnoreCase(level)) {
                    warnCount++;
                }
                
                Map<String, Object> bulletinInfo = new HashMap<>();
                bulletinInfo.put("level", level);
                bulletinInfo.put("message", bulletin.path("message").asText());
                bulletinInfo.put("sourceName", bulletin.path("sourceName").asText());
                bulletinInfo.put("timestamp", bulletin.path("timestamp").asText());
                
                recentBulletins.add(bulletinInfo);
                
                if (recentBulletins.size() >= 20) {
                    break;
                }
            }
            
            result.put("bulletins", recentBulletins);
            result.put("bulletinErrorCount", errorCount);
            result.put("bulletinWarnCount", warnCount);
            
        } catch (Exception e) {
            log.debug("Failed to extract bulletins: {}", e.getMessage());
        }
    }
    
    /**
     * 커넥션/큐 상태 추출
     */
    private void extractConnectionStatus(Map<String, Object> result) {
        try {
            JsonNode processGroup = callNiFiApi(PROCESS_GROUP_STATUS_ENDPOINT);
            if (processGroup == null) {
                return;
            }
            
            JsonNode connectionStatus = processGroup.path("processGroupStatus")
                .path("aggregateSnapshot").path("connectionStatusSnapshots");
            
            if (!connectionStatus.isArray() || connectionStatus.size() == 0) {
                return;
            }
            
            int totalQueued = 0;
            long totalQueuedBytes = 0;
            int backpressureCount = 0;
            
            for (JsonNode conn : connectionStatus) {
                JsonNode snapshot = conn.has("connectionStatusSnapshot") 
                    ? conn.path("connectionStatusSnapshot") 
                    : conn;
                
                // 큐 정보 집계
                int flowFilesQueued = snapshot.path("flowFilesQueued").asInt();
                long bytesQueued = snapshot.path("bytesQueued").asLong();
                
                totalQueued += flowFilesQueued;
                totalQueuedBytes += bytesQueued;
                
                // 백프레셔 상태 확인
                int percentUseBytes = snapshot.path("percentUseBytes").asInt();
                int percentUseCount = snapshot.path("percentUseCount").asInt();
                if (percentUseBytes >= 100 || percentUseCount >= 100) {
                    backpressureCount++;
                }
            }
            
            result.put("totalQueuedFlowFiles", totalQueued);
            result.put("totalQueuedBytes", totalQueuedBytes);
            result.put("totalQueuedBytesFormatted", NiFiMetricsUtils.formatBytes(totalQueuedBytes));
            result.put("backpressureCount", backpressureCount);
            
        } catch (Exception e) {
            log.debug("Failed to extract connection status: {}", e.getMessage());
        }
    }

    /**
     * 처리량 계산 및 변화 감지
     */
    private void calculateThroughputAndChanges(Map<String, Object> result) {
        Map<String, Object> throughput = new HashMap<>();
        
        @SuppressWarnings("unchecked")
        Map<String, Object> flow = (Map<String, Object>) result.get("flow");
        
        if (flow == null) {
            result.put("throughput", throughput);
            return;
        }
        
        // 현재 값
        long bytesIn = ((Number) flow.getOrDefault("bytesIn", 0L)).longValue();
        long bytesOut = ((Number) flow.getOrDefault("bytesOut", 0L)).longValue();
        int flowFilesIn = ((Number) flow.getOrDefault("flowFilesIn", 0)).intValue();
        int flowFilesOut = ((Number) flow.getOrDefault("flowFilesOut", 0)).intValue();
        
        throughput.put("flowFilesIn", flowFilesIn);
        throughput.put("flowFilesOut", flowFilesOut);
        throughput.put("bytesInFormatted", NiFiMetricsUtils.formatBytes(bytesIn));
        throughput.put("bytesOutFormatted", NiFiMetricsUtils.formatBytes(bytesOut));
        
        // 활동 여부 확인
        boolean hasCurrentActivity = flowFilesIn > 0 || flowFilesOut > 0;
        throughput.put("hasFlowData", hasCurrentActivity);
        
        // 이전 값과 비교하여 변화 감지
        Long prevFlowFilesIn = previousFlowMetrics.get("flowFilesIn");
        Long prevFlowFilesOut = previousFlowMetrics.get("flowFilesOut");
        
        if (prevFlowFilesIn != null && prevFlowFilesOut != null) {
            int deltaIn = flowFilesIn - prevFlowFilesIn.intValue();
            int deltaOut = flowFilesOut - prevFlowFilesOut.intValue();
            
            boolean hasRecentActivity = deltaIn > 0 || deltaOut > 0;
            throughput.put("hasRecentActivity", hasRecentActivity);
            
            if (hasRecentActivity) {
                lastProcessingTime = Instant.now();
            }
        } else {
            // 첫 실행: 이전 값이 없으면 현재 활동 상태 확인
            throughput.put("hasRecentActivity", hasCurrentActivity);
            if (hasCurrentActivity) {
                // 현재 활동이 있으면 마지막 처리 시간 설정
                lastProcessingTime = Instant.now();
            } else {
                // 프로세서별 활동이 있는지 확인하여 lastProcessingTime 업데이트
                @SuppressWarnings("unchecked")
                Map<String, Map<String, Object>> processors = 
                    (Map<String, Map<String, Object>>) result.get("processors");
                if (processors != null) {
                    for (Map<String, Object> processor : processors.values()) {
                        if (Boolean.TRUE.equals(processor.get("hasActivity"))) {
                            // 프로세서에 활동이 있으면 마지막 처리 시간 설정
                            if (lastProcessingTime == null) {
                                lastProcessingTime = Instant.now();
                            }
                            break;
                        }
                    }
                }
            }
        }
        
        // 현재 값 저장
        previousFlowMetrics.put("flowFilesIn", (long) flowFilesIn);
        previousFlowMetrics.put("flowFilesOut", (long) flowFilesOut);
        
        // 마지막 처리 시간
        if (lastProcessingTime != null) {
            throughput.put("lastProcessingTime", NiFiMetricsUtils.DATETIME_FORMATTER.format(lastProcessingTime));
            Duration sinceLast = Duration.between(lastProcessingTime, Instant.now());
            throughput.put("timeSinceLastProcessing", NiFiMetricsUtils.formatDuration(sinceLast));
        } else {
            throughput.put("lastProcessingTime", "N/A");
            throughput.put("timeSinceLastProcessing", "데이터 없음");
        }
        
        result.put("throughput", throughput);
    }

    /**
     * 요약 메트릭 생성
     */
    private void generateSummaryMetrics(Map<String, Object> result) {
        Map<String, Object> summary = new LinkedHashMap<>();
        
        @SuppressWarnings("unchecked")
        Map<String, Object> flow = (Map<String, Object>) result.get("flow");
        @SuppressWarnings("unchecked")
        Map<String, Object> throughput = (Map<String, Object>) result.get("throughput");
        @SuppressWarnings("unchecked")
        Map<String, Object> system = (Map<String, Object>) result.get("system");
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> processors = (Map<String, Map<String, Object>>) result.get("processors");
        
        // 프로세서 상태 집계
        int totalProcessors = 0;
        int runningCount = 0;
        int activeCount = 0;
        int processorsWithData = 0;
        
        if (processors != null) {
            for (Map<String, Object> proc : processors.values()) {
                totalProcessors++;
                
                String status = String.valueOf(proc.get("runStatus"));
                if ("Running".equalsIgnoreCase(status)) {
                    runningCount++;
                }
                
                if (Boolean.TRUE.equals(proc.get("isActive"))) {
                    activeCount++;
                }
                
                if (Boolean.TRUE.equals(proc.get("hasActivity"))) {
                    processorsWithData++;
                }
            }
        }
        
        summary.put("totalProcessors", totalProcessors);
        summary.put("runningProcessors", runningCount);
        summary.put("activeProcessors", activeCount);
        summary.put("processorsWithData", processorsWithData);
        
        // 플로우 상태
        if (flow != null) {
            summary.put("queuedCount", flow.getOrDefault("queuedCount", 0));
            summary.put("activeThreads", flow.getOrDefault("activeThreadCount", 0));
        }
        
        // 처리량 정보
        if (throughput != null) {
            summary.put("totalFlowFilesIn", throughput.getOrDefault("flowFilesIn", 0));
            summary.put("totalFlowFilesOut", throughput.getOrDefault("flowFilesOut", 0));
            summary.put("totalBytesIn", throughput.getOrDefault("bytesInFormatted", "0 B"));
            summary.put("totalBytesOut", throughput.getOrDefault("bytesOutFormatted", "0 B"));
            summary.put("lastProcessingTime", throughput.getOrDefault("lastProcessingTime", "N/A"));
            summary.put("timeSinceLastProcessing", throughput.getOrDefault("timeSinceLastProcessing", "N/A"));
            summary.put("hasRecentActivity", throughput.getOrDefault("hasRecentActivity", false));
        }
        
        // 시스템 활동 상태
        boolean isSystemActive = activeCount > 0 || processorsWithData > 0;
        summary.put("isSystemActive", isSystemActive);
        
        // 시스템 리소스
        if (system != null) {
            summary.put("heapUsagePercent", system.getOrDefault("heapUsagePercent", 0));
            summary.put("cpuLoad", system.getOrDefault("cpuLoadAverage", 0));
            summary.put("uptime", system.getOrDefault("uptime", "N/A"));
        }
        
        // 알림 및 백프레셔
        summary.put("bulletinErrorCount", result.getOrDefault("bulletinErrorCount", 0));
        summary.put("bulletinWarnCount", result.getOrDefault("bulletinWarnCount", 0));
        summary.put("backpressureCount", result.getOrDefault("backpressureCount", 0));
        
        result.put("summary", summary);
    }

    /**
     * Status History API를 사용하여 프로세서의 실제 마지막 활동 시간 추적
     * 
     * @param processorId 프로세서 ID
     * @param groupId 프로세서가 속한 그룹 ID
     * @return 마지막 활동 시간 (찾지 못하면 null)
     */
    private Instant getLastActivityTimeFromHistory(String processorId, String groupId) {
        try {
            // Status History API 호출
            JsonNode historyResponse = callNiFiApi("/flow/process-groups/" + groupId + "/status/history");
            if (historyResponse == null) {
                return null;
            }
            
            JsonNode statusHistory = historyResponse.path("statusHistory");
            if (statusHistory.isMissingNode()) {
                return null;
            }
            
            // aggregateSnapshots에서 가장 최근의 활동 시간 찾기
            JsonNode aggregateSnapshots = statusHistory.path("aggregateSnapshots");
            if (aggregateSnapshots.isArray() && aggregateSnapshots.size() > 0) {
                // 가장 최근 스냅샷부터 확인
                for (int i = aggregateSnapshots.size() - 1; i >= 0; i--) {
                    JsonNode snapshot = aggregateSnapshots.get(i);
                    
                    // 프로세서별 메트릭이 있는지 확인
                    // Status History는 프로세서별로 직접 제공하지 않을 수 있으므로
                    // 프로세스 그룹 레벨의 활동 시간을 사용
                    String timestampStr = snapshot.path("timestamp").asText();
                    if (!timestampStr.isEmpty()) {
                        try {
                            Instant timestamp = Instant.parse(timestampStr);
                            // 활동이 있었던 가장 최근 시간 반환
                            return timestamp;
                        } catch (Exception e) {
                            log.debug("Failed to parse timestamp from status history: {}", timestampStr);
                        }
                    }
                }
            }
            
            // nodeSnapshots에서도 확인 시도
            JsonNode nodeSnapshots = statusHistory.path("nodeSnapshots");
            if (nodeSnapshots.isArray() && nodeSnapshots.size() > 0) {
                for (JsonNode nodeSnapshot : nodeSnapshots) {
                    JsonNode statusSnapshots = nodeSnapshot.path("statusSnapshots");
                    if (statusSnapshots.isArray() && statusSnapshots.size() > 0) {
                        // 가장 최근 스냅샷 확인
                        JsonNode latestSnapshot = statusSnapshots.get(statusSnapshots.size() - 1);
                        if (latestSnapshot != null && !latestSnapshot.isNull()) {
                            // 타임스탬프가 있으면 사용
                            JsonNode timestampNode = latestSnapshot.path("timestamp");
                            if (!timestampNode.isMissingNode()) {
                                String timestampStr = timestampNode.asText();
                                if (!timestampStr.isEmpty() && !timestampStr.equals("null")) {
                                    try {
                                        return Instant.parse(timestampStr);
                                    } catch (Exception e) {
                                        // 무시
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
        } catch (Exception e) {
            log.debug("Failed to get last activity time from history for processor {}: {}", processorId, e.getMessage());
        }
        
        return null;
    }

}
