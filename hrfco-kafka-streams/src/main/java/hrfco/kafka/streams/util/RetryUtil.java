package hrfco.kafka.streams.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Exponential backoff 재시도 유틸리티
 */
public class RetryUtil {
    private static final Logger logger = LoggerFactory.getLogger(RetryUtil.class);

    private static final int DEFAULT_MAX_RETRIES = Integer.parseInt(
        System.getenv().getOrDefault("RETRY_MAX_ATTEMPTS", "3"));
    private static final long DEFAULT_INITIAL_DELAY_MS = Long.parseLong(
        System.getenv().getOrDefault("RETRY_INITIAL_DELAY_MS", "500"));

    /**
     * Supplier 실행 (반환값 있는 작업)
     */
    public static <T> T executeWithRetry(Supplier<T> operation, String operationName) {
        return executeWithRetry(operation, DEFAULT_MAX_RETRIES, DEFAULT_INITIAL_DELAY_MS, operationName);
    }

    /**
     * Runnable 실행 (반환값 없는 작업)
     */
    public static void executeWithRetry(Runnable operation, String operationName) {
        executeWithRetry(operation, DEFAULT_MAX_RETRIES, DEFAULT_INITIAL_DELAY_MS, operationName);
    }

    /**
     * Supplier 실행 (설정 가능)
     */
    public static <T> T executeWithRetry(Supplier<T> operation, int maxRetries,
                                          long initialDelayMs, String operationName) {
        Exception lastException = null;
        long delay = initialDelayMs;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return operation.get();
            } catch (Exception e) {
                lastException = e;
                if (attempt < maxRetries) {
                    logger.warn("[{}] Attempt {}/{} failed, retrying in {}ms: {}",
                        operationName, attempt, maxRetries, delay, e.getMessage());
                    sleep(delay);
                    delay *= 2; // exponential backoff
                } else {
                    logger.error("[{}] All {} attempts failed", operationName, maxRetries, e);
                }
            }
        }

        throw new RuntimeException(
            String.format("[%s] Failed after %d attempts", operationName, maxRetries), lastException);
    }

    /**
     * Runnable 실행 (설정 가능)
     */
    public static void executeWithRetry(Runnable operation, int maxRetries,
                                         long initialDelayMs, String operationName) {
        executeWithRetry(() -> {
            operation.run();
            return null;
        }, maxRetries, initialDelayMs, operationName);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Retry interrupted", e);
        }
    }
}
