package hrfco.kafka.streams.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 3중 저장소 저장 결과
 * 각 백엔드별 성공/실패 상태를 추적
 */
public class StorageResult {
    private boolean minIOSuccess;
    private boolean timescaleDBSuccess;
    private boolean mongoDBSuccess;
    private final List<String> failures = new ArrayList<>();
    private final List<String> errors = new ArrayList<>();

    public StorageResult() {
        this.minIOSuccess = true;
        this.timescaleDBSuccess = true;
        this.mongoDBSuccess = true;
    }

    public void markMinIOFailure(String error) {
        this.minIOSuccess = false;
        this.failures.add("MinIO");
        this.errors.add("MinIO: " + error);
    }

    public void markTimescaleDBFailure(String error) {
        this.timescaleDBSuccess = false;
        this.failures.add("TimescaleDB");
        this.errors.add("TimescaleDB: " + error);
    }

    public void markMongoDBFailure(String error) {
        this.mongoDBSuccess = false;
        this.failures.add("MongoDB");
        this.errors.add("MongoDB: " + error);
    }

    public boolean isAllSuccess() {
        return minIOSuccess && timescaleDBSuccess && mongoDBSuccess;
    }

    public boolean isAllFailed() {
        return !minIOSuccess && !timescaleDBSuccess && !mongoDBSuccess;
    }

    public boolean isPartialFailure() {
        return !isAllSuccess() && !isAllFailed();
    }

    public int getSuccessCount() {
        int count = 0;
        if (minIOSuccess) count++;
        if (timescaleDBSuccess) count++;
        if (mongoDBSuccess) count++;
        return count;
    }

    public List<String> getFailures() {
        return Collections.unmodifiableList(failures);
    }

    public List<String> getErrors() {
        return Collections.unmodifiableList(errors);
    }

    public boolean isMinIOSuccess() { return minIOSuccess; }
    public boolean isTimescaleDBSuccess() { return timescaleDBSuccess; }
    public boolean isMongoDBSuccess() { return mongoDBSuccess; }

    @Override
    public String toString() {
        return String.format("StorageResult{success=%d/3, failures=%s}",
            getSuccessCount(), failures);
    }
}
