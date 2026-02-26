// 관측소 임계값 데이터 (캐시용)
package hrfco.kafka.streams.cache;

import org.bson.Document;

/**
 * 관측소별 홍수 경보 임계값 데이터
 */
public class StationThreshold {
    private final Double attentionThreshold;  // 관심 수위 (attwl) - NULL 가능
    private final Double cautionThreshold;    // 주의 수위 (wrnwl) - NULL 가능
    private final Double warningThreshold;    // 경계 수위 (almwl) - NULL 가능
    private final Double criticalThreshold;   // 심각 수위 (srswl) - NULL 가능
    
    public StationThreshold(Double attentionThreshold, Double cautionThreshold,
                           Double warningThreshold, Double criticalThreshold) {
        this.attentionThreshold = attentionThreshold;
        this.cautionThreshold = cautionThreshold;
        this.warningThreshold = warningThreshold;
        this.criticalThreshold = criticalThreshold;
    }
    
    /**
     * MongoDB Document에서 StationThreshold 생성
     */
    public static StationThreshold fromDocument(Document doc) {
        if (doc == null) {
            return null;
        }
        
        Double attwl = getDoubleOrNull(doc, "attwl");
        Double wrnwl = getDoubleOrNull(doc, "wrnwl");
        Double almwl = getDoubleOrNull(doc, "almwl");
        Double srswl = getDoubleOrNull(doc, "srswl");
        
        return new StationThreshold(attwl, wrnwl, almwl, srswl);
    }
    
    private static Double getDoubleOrNull(Document doc, String key) {
        Object value = doc.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Double) {
            return (Double) value;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            String str = ((String) value).trim();
            if (str.isEmpty() || str.equals(" ")) {
                return null;
            }
            try {
                return Double.parseDouble(str);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
    
    // Getters
    public Double getAttentionThreshold() { return attentionThreshold; }
    public Double getCautionThreshold() { return cautionThreshold; }
    public Double getWarningThreshold() { return warningThreshold; }
    public Double getCriticalThreshold() { return criticalThreshold; }
    
    @Override
    public String toString() {
        return String.format("StationThreshold{attention=%s, caution=%s, warning=%s, critical=%s}",
                attentionThreshold, cautionThreshold, warningThreshold, criticalThreshold);
    }
}

