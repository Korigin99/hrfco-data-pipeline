package com.magpie.hrfco_monitoring_server.service;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * NiFi 메트릭 관련 유틸리티 클래스
 */
public class NiFiMetricsUtils {
    
    public static final ZoneId KOREA_ZONE = ZoneId.of("Asia/Seoul");
    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(KOREA_ZONE);
    public static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(KOREA_ZONE);
    
    /**
     * 바이트를 포맷된 문자열로 변환
     */
    public static String formatBytes(long bytes) {
        if (bytes < 0) {
            return "0 B";
        }
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        } else {
            return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
        }
    }
    
    /**
     * 포맷된 바이트 문자열 파싱
     */
    public static long parseFormattedBytes(String formatted) {
        if (formatted == null || formatted.isEmpty() || formatted.equals("0 B")) {
            return 0L;
        }
        
        try {
            if (formatted.contains("(") && formatted.contains(")")) {
                int startIdx = formatted.indexOf("(");
                int endIdx = formatted.indexOf(")");
                if (startIdx < endIdx) {
                    formatted = formatted.substring(startIdx + 1, endIdx).trim();
                }
            }
            
            String[] parts = formatted.trim().split("\\s+");
            if (parts.length < 2) {
                return Long.parseLong(formatted.trim());
            }
            
            double value = Double.parseDouble(parts[0]);
            String unit = parts[1].toUpperCase();
            
            switch (unit) {
                case "B":
                case "BYTES":
                    return (long) value;
                case "KB":
                case "KILOBYTES":
                    return (long) (value * 1024);
                case "MB":
                case "MEGABYTES":
                    return (long) (value * 1024 * 1024);
                case "GB":
                case "GIGABYTES":
                    return (long) (value * 1024 * 1024 * 1024);
                case "TB":
                case "TERABYTES":
                    return (long) (value * 1024L * 1024 * 1024 * 1024);
                default:
                    return 0L;
            }
        } catch (Exception e) {
            return 0L;
        }
    }
    
    /**
     * Duration을 읽기 쉬운 형식으로 변환
     */
    public static String formatDuration(Duration duration) {
        long seconds = duration.getSeconds();
        
        if (seconds < 60) {
            return seconds + "초 전";
        } else if (seconds < 3600) {
            return (seconds / 60) + "분 " + (seconds % 60) + "초 전";
        } else if (seconds < 86400) {
            long hours = seconds / 3600;
            long mins = (seconds % 3600) / 60;
            return hours + "시간 " + mins + "분 전";
        } else {
            long days = seconds / 86400;
            long hours = (seconds % 86400) / 3600;
            return days + "일 " + hours + "시간 전";
        }
    }
    
    /**
     * 큐 문자열에서 개수 파싱
     * 예: "0 (0 bytes)" -> 0
     */
    public static int parseQueuedCount(String queued) {
        if (queued == null || queued.isEmpty()) {
            return 0;
        }
        try {
            String countPart = queued.split("\\s+")[0].replace(",", "");
            return Integer.parseInt(countPart);
        } catch (Exception e) {
            return 0;
        }
    }
    
    /**
     * NiFi 타임스탬프 파싱
     */
    public static Instant parseNiFiTimestamp(String timestamp) {
        try {
            if (timestamp.contains("/")) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss.SSS")
                    .withZone(ZoneId.systemDefault());
                return Instant.from(formatter.parse(timestamp));
            } else if (timestamp.contains("T")) {
                return Instant.parse(timestamp);
            }
        } catch (Exception e) {
            // 파싱 실패 시 현재 시간 반환
        }
        return Instant.now();
    }
}

