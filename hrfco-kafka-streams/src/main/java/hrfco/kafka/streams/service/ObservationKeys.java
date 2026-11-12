package hrfco.kafka.streams.service;

import hrfco.kafka.streams.model.WaterLevelData;

/**
 * 관측값 하나를 가리키는 멱등 키
 * 키 = 관측소 코드 + 관측 시각(yyyyMMddHHmm). 같은 관측값은 몇 번을 저장해도 같은 위치에 쓴다.
 * 처리 시각·현재 밀리초처럼 저장할 때마다 바뀌는 값은 키에 넣지 않는다.
 */
public final class ObservationKeys {

    private ObservationKeys() {
    }

    /** MongoDB _id: {관측소코드}_{yyyyMMddHHmm} */
    public static String documentId(WaterLevelData data) {
        return data.getObservationCode() + "_" + observationTime(data);
    }

    /** MinIO 경로: waterlevel/{normal|anomalies}/{관측일 yyyy/MM/dd}/obs_{코드}_{yyyyMMddHHmm}.json */
    public static String minioPath(WaterLevelData data, boolean isAnomaly) {
        String ymdhm = observationTime(data);
        return String.format("waterlevel/%s/%s/%s/%s/obs_%s_%s.json",
            isAnomaly ? "anomalies" : "normal",
            ymdhm.substring(0, 4), ymdhm.substring(4, 6), ymdhm.substring(6, 8),
            data.getObservationCode(), ymdhm);
    }

    /**
     * 반대쪽 폴더 경로. 정정값으로 이상치 판정이 바뀌면 폴더가 달라져 키가 둘이 되므로,
     * 저장 후 반대쪽 객체를 지워 관측값 하나에 객체 하나를 유지한다.
     */
    public static String counterpartMinioPath(WaterLevelData data, boolean isAnomaly) {
        return minioPath(data, !isAnomaly);
    }

    private static String observationTime(WaterLevelData data) {
        String ymdhm = data.getObservationTime();
        if (ymdhm == null || !ymdhm.matches("\\d{12}")) {
            throw new IllegalArgumentException("observation time must be yyyyMMddHHmm: " + ymdhm);
        }
        return ymdhm;
    }
}
