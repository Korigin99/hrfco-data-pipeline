package com.magpie.hrfco_monitoring_server.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@Service
@Slf4j
public class AlertService {
    
    // 관측소별 구독자 관리
    private final Map<String, CopyOnWriteArraySet<SseEmitter>> subscribers = new ConcurrentHashMap<>();
    
    /**
     * SSE 구독
     * 
     * @param station 관측소 코드 (null이면 모든 알림 수신)
     * @return SseEmitter
     */
    public SseEmitter subscribe(String station) {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        
        String key = station != null ? station : "all";
        subscribers.computeIfAbsent(key, k -> new CopyOnWriteArraySet<>()).add(emitter);
        
        log.info("Client subscribed. Station: {}, Total subscribers: {}", 
                key, subscribers.values().stream().mapToInt(CopyOnWriteArraySet::size).sum());
        
        // 연결 종료 시 정리
        emitter.onCompletion(() -> {
            subscribers.get(key).remove(emitter);
            log.info("Client disconnected. Station: {}", key);
        });
        
        emitter.onError((ex) -> {
            subscribers.get(key).remove(emitter);
            log.error("SSE error for station: {}", key, ex);
        });
        
        emitter.onTimeout(() -> {
            subscribers.get(key).remove(emitter);
            log.warn("SSE timeout for station: {}", key);
        });
        
        // 초기 연결 메시지
        try {
            emitter.send(SseEmitter.event()
                    .name("connected")
                    .data("{\"status\":\"connected\",\"station\":\"" + key + "\"}"));
        } catch (IOException e) {
            log.error("Failed to send initial message", e);
        }
        
        return emitter;
    }
    
    /**
     * 알림 브로드캐스트
     * 
     * @param station 관측소 코드
     * @param alertJson 알림 JSON
     */
    public void broadcastAlert(String station, String alertJson) {
        // 모든 구독자에게 전송
        sendToSubscribers("all", alertJson);
        
        // 특정 관측소 구독자에게 전송
        if (station != null) {
            sendToSubscribers(station, alertJson);
        }
    }
    
    private void sendToSubscribers(String key, String data) {
        CopyOnWriteArraySet<SseEmitter> emitters = subscribers.get(key);
        if (emitters == null || emitters.isEmpty()) {
            return;
        }
        
        emitters.removeIf(emitter -> {
            try {
                emitter.send(SseEmitter.event()
                        .name("alert")
                        .data(data));
                return false;
            } catch (IOException e) {
                log.warn("Failed to send alert to subscriber", e);
                return true; // 실패한 emitter 제거
            }
        });
    }
    
    /**
     * 현재 구독자 수 조회
     */
    public int getSubscriberCount() {
        return subscribers.values().stream().mapToInt(CopyOnWriteArraySet::size).sum();
    }
}



