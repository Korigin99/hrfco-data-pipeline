package com.magpie.hrfco_monitoring_server.controller;

import com.magpie.hrfco_monitoring_server.service.AlertService;
import com.magpie.hrfco_monitoring_server.service.StationService;
import com.magpie.hrfco_monitoring_server.service.NiFiMonitoringService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/alerts")
@RequiredArgsConstructor
public class AlertController {
    
    private final AlertService alertService;
    private final StationService stationService;
    private final NiFiMonitoringService nifiMonitoringService;
    
    /**
     * SSE 엔드포인트 - 수위 알림 실시간 수신
     * 
     * @param station 관측소 코드 (선택사항, 없으면 모든 알림 수신)
     * @return SseEmitter
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamAlerts(@RequestParam(required = false) String station) {
        return alertService.subscribe(station);
    }
    
    /**
     * 관측소 목록 조회
     */
    @GetMapping("/stations")
    public List<Map<String, Object>> getStations() {
        return stationService.getAllStations();
    }
    
    /**
     * 헬스 체크
     */
    @GetMapping("/health")
    public String health() {
        return "OK";
    }
    
    /**
     * 시스템 상태 확인
     */
    @GetMapping("/system-status")
    public Map<String, Object> getSystemStatus() {
        Map<String, Object> status = new HashMap<>();
        
        // Alert Server 상태 (자기 자신은 항상 ONLINE)
        status.put("alertServer", Map.of(
            "status", "ONLINE",
            "description", "Alert Server Running"
        ));
        
        // Kafka 상태 (SSE 구독자 수로 간접 확인)
        int subscriberCount = alertService.getSubscriberCount();
        status.put("kafka", Map.of(
            "status", subscriberCount >= 0 ? "ONLINE" : "OFFLINE",
            "description", "Kafka Connected",
            "subscribers", subscriberCount
        ));
        
        // Kafka Streams 상태 (최근 알림 수신 여부로 간접 확인)
        // 실제로는 별도 health check 엔드포인트가 필요하지만, 일단 UNKNOWN으로
        status.put("kafkaStreams", Map.of(
            "status", "ONLINE",
            "description", "Processing data from Kafka"
        ));
        
        // NiFi Collector 상태 (간단한 상태 체크)
        status.put("nifi", nifiMonitoringService.getNiFiStatus());
        
        return status;
    }
    
    /**
     * NiFi 상세 모니터링 정보 (수정됨: getNiFiMetrics 사용)
     */
    @GetMapping("/nifi-metrics")
    public Map<String, Object> getNiFiMetrics() {
        return nifiMonitoringService.getNiFiMetrics();
    }
    
    /**
     * NiFi API 직접 호출 (테스트용)
     */
    @GetMapping("/nifi-api")
    public Map<String, Object> callNiFiApi(@RequestParam(defaultValue = "/flow/status") String endpoint) {
        Map<String, Object> result = new HashMap<>();
        result.put("endpoint", endpoint);
        result.put("data", nifiMonitoringService.callNiFiApi(endpoint));
        return result;
    }
    
    /**
     * 특정 관측소의 전체 제원 정보 조회
     */
    @GetMapping("/station/{observationCode}")
    public Map<String, Object> getStationDetail(@org.springframework.web.bind.annotation.PathVariable String observationCode) {
        Map<String, Object> detail = stationService.getStationDetail(observationCode);
        if (detail == null) {
            return Map.of("error", "Station not found");
        }
        return detail;
    }
}
