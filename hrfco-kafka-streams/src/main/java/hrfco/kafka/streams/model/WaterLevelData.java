// 한강 홍수통제소 수위 데이터 모델
// API: https://api.hrfco.go.kr/

package hrfco.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WaterLevelData {
    // 관측소 코드 (예: "1001602")
    @JsonProperty("wlobscd")
    private String observationCode;
    
    // 관측 시간 (yyyyMMddHHmm 형식, 예: "202512041530")
    @JsonProperty("ymdhm")
    private String observationTime;
    
    // 수위 (m) - 문자형으로 받아서 Double로 변환
    @JsonProperty("wl")
    private String waterLevel;
    
    // 유량 (CMS - Cubic Meter per Second) - 값이 없을 수 있음
    @JsonProperty("fw")
    private String flowRate;
    
    // Getters and Setters
    
    public String getObservationCode() {
        return observationCode;
    }
    
    public void setObservationCode(String observationCode) {
        this.observationCode = observationCode;
    }
    
    public String getWaterLevel() {
        return waterLevel;
    }
    
    public void setWaterLevel(String waterLevel) {
        this.waterLevel = waterLevel;
    }
    
    public String getObservationTime() {
        return observationTime;
    }
    
    public void setObservationTime(String observationTime) {
        this.observationTime = observationTime;
    }
    
    public String getFlowRate() {
        return flowRate;
    }
    
    public void setFlowRate(String flowRate) {
        this.flowRate = flowRate;
    }
    
    // 편의 메서드: 문자형 수위를 Double로 변환
    public Double getWaterLevelValue() {
        try {
            return waterLevel != null && !waterLevel.trim().isEmpty() && !waterLevel.trim().equals(" ") 
                    ? Double.parseDouble(waterLevel.trim()) : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    // 편의 메서드: 문자형 유량을 Double로 변환
    public Double getFlowRateValue() {
        try {
            return flowRate != null && !flowRate.trim().isEmpty() && !flowRate.trim().equals(" ") 
                    ? Double.parseDouble(flowRate.trim()) : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    // 편의 메서드: 관측 시간 파싱 (yyyyMMddHHmm → yyyy-MM-dd HH:mm)
    public String getFormattedObservationTime() {
        try {
            if (observationTime != null && observationTime.length() == 12) {
                String year = observationTime.substring(0, 4);
                String month = observationTime.substring(4, 6);
                String day = observationTime.substring(6, 8);
                String hour = observationTime.substring(8, 10);
                String minute = observationTime.substring(10, 12);
                return String.format("%s-%s-%s %s:%s", year, month, day, hour, minute);
            }
            return observationTime;
        } catch (Exception e) {
            return observationTime;
        }
    }
    
    @Override
    public String toString() {
        return "WaterLevelData{" +
                "observationCode='" + observationCode + '\'' +
                ", observationTime='" + observationTime + '\'' +
                ", waterLevel='" + waterLevel + '\'' +
                ", flowRate='" + flowRate + '\'' +
                '}';
    }
}

