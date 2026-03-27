package hrfco.kafka.streams.resilience;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * 저장소별 Circuit Breaker + Retry 관리
 * 백엔드가 지속적으로 실패할 경우 요청을 차단하여 시스템 보호
 */
public class StorageCircuitBreakerRegistry {
    private static final Logger logger = LoggerFactory.getLogger(StorageCircuitBreakerRegistry.class);

    private final CircuitBreaker minIOBreaker;
    private final CircuitBreaker timescaleDBBreaker;
    private final CircuitBreaker mongoDBBreaker;

    private final Retry minIORetry;
    private final Retry timescaleDBRetry;
    private final Retry mongoDBRetry;

    public StorageCircuitBreakerRegistry() {
        // Circuit Breaker 설정
        int failureRateThreshold = Integer.parseInt(
            System.getenv().getOrDefault("CB_FAILURE_RATE_THRESHOLD", "50"));
        int waitDurationSeconds = Integer.parseInt(
            System.getenv().getOrDefault("CB_WAIT_DURATION_SECONDS", "30"));
        int slidingWindowSize = Integer.parseInt(
            System.getenv().getOrDefault("CB_SLIDING_WINDOW_SIZE", "10"));
        int minimumCalls = Integer.parseInt(
            System.getenv().getOrDefault("CB_MINIMUM_CALLS", "5"));

        CircuitBreakerConfig cbConfig = CircuitBreakerConfig.custom()
            .failureRateThreshold(failureRateThreshold)
            .waitDurationInOpenState(Duration.ofSeconds(waitDurationSeconds))
            .slidingWindowSize(slidingWindowSize)
            .minimumNumberOfCalls(minimumCalls)
            .build();

        CircuitBreakerRegistry cbRegistry = CircuitBreakerRegistry.of(cbConfig);

        this.minIOBreaker = cbRegistry.circuitBreaker("minio");
        this.timescaleDBBreaker = cbRegistry.circuitBreaker("timescaledb");
        this.mongoDBBreaker = cbRegistry.circuitBreaker("mongodb");

        // 상태 변경 이벤트 리스너
        registerStateChangeListener(minIOBreaker, "MinIO");
        registerStateChangeListener(timescaleDBBreaker, "TimescaleDB");
        registerStateChangeListener(mongoDBBreaker, "MongoDB");

        // Retry 설정
        int maxRetries = Integer.parseInt(
            System.getenv().getOrDefault("RETRY_MAX_ATTEMPTS", "3"));
        long initialDelayMs = Long.parseLong(
            System.getenv().getOrDefault("RETRY_INITIAL_DELAY_MS", "500"));

        RetryConfig retryConfig = RetryConfig.custom()
            .maxAttempts(maxRetries)
            .waitDuration(Duration.ofMillis(initialDelayMs))
            .build();

        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);

        this.minIORetry = retryRegistry.retry("minio");
        this.timescaleDBRetry = retryRegistry.retry("timescaledb");
        this.mongoDBRetry = retryRegistry.retry("mongodb");

        logger.info("StorageCircuitBreakerRegistry initialized - failureRate: {}%, waitDuration: {}s, windowSize: {}, retries: {}",
            failureRateThreshold, waitDurationSeconds, slidingWindowSize, maxRetries);
    }

    private void registerStateChangeListener(CircuitBreaker breaker, String name) {
        breaker.getEventPublisher().onStateTransition(event ->
            logger.warn("Circuit Breaker [{}] state changed: {} -> {}",
                name, event.getStateTransition().getFromState(), event.getStateTransition().getToState()));
    }

    /**
     * MinIO 작업 실행 (Retry + Circuit Breaker)
     */
    public void executeMinIO(Runnable operation) {
        Supplier<Void> supplier = CircuitBreaker.decorateSupplier(minIOBreaker, () -> {
            operation.run();
            return null;
        });
        supplier = Retry.decorateSupplier(minIORetry, supplier);
        supplier.get();
    }

    /**
     * TimescaleDB 작업 실행 (Retry + Circuit Breaker)
     */
    public void executeTimescaleDB(Runnable operation) {
        Supplier<Void> supplier = CircuitBreaker.decorateSupplier(timescaleDBBreaker, () -> {
            operation.run();
            return null;
        });
        supplier = Retry.decorateSupplier(timescaleDBRetry, supplier);
        supplier.get();
    }

    /**
     * MongoDB 작업 실행 (Retry + Circuit Breaker)
     */
    public void executeMongoDB(Runnable operation) {
        Supplier<Void> supplier = CircuitBreaker.decorateSupplier(mongoDBBreaker, () -> {
            operation.run();
            return null;
        });
        supplier = Retry.decorateSupplier(mongoDBRetry, supplier);
        supplier.get();
    }

    public CircuitBreaker.State getMinIOState() { return minIOBreaker.getState(); }
    public CircuitBreaker.State getTimescaleDBState() { return timescaleDBBreaker.getState(); }
    public CircuitBreaker.State getMongoDBState() { return mongoDBBreaker.getState(); }
}
