package hrfco.kafka.streams.resilience;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class StorageCircuitBreakerRegistryTest {

    @Test
    void openBreaker_failsFastWithoutRetryWait() {
        // 기본값: 재시도 3회(500ms 간격), 최소 호출 5회·실패율 50%에서 OPEN
        StorageCircuitBreakerRegistry registry = new StorageCircuitBreakerRegistry();
        Runnable failing = () -> {
            throw new RuntimeException("storage down");
        };
        for (int i = 0; i < 2; i++) {
            assertThrows(RuntimeException.class, () -> registry.executeMinIO(failing));
        }
        assertEquals(CircuitBreaker.State.OPEN, registry.getMinIOState());

        AtomicInteger calls = new AtomicInteger();
        long start = System.nanoTime();
        assertThrows(CallNotPermittedException.class, () -> registry.executeMinIO(calls::incrementAndGet));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        // 열린 브레이커의 거절은 재시도해도 같은 결과라, 대기 없이 바로 실패해야 한다
        assertEquals(0, calls.get());
        assertTrue(elapsedMs < 300, "expected fail-fast but took " + elapsedMs + "ms");
    }
}
