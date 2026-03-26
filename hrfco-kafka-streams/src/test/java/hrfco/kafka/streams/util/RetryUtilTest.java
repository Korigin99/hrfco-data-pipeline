package hrfco.kafka.streams.util;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RetryUtilTest {

    @Test
    void executeWithRetry_succeedsOnFirstAttempt() {
        AtomicInteger attempts = new AtomicInteger(0);

        String result = RetryUtil.executeWithRetry(() -> {
            attempts.incrementAndGet();
            return "success";
        }, 3, 10, "test-op");

        assertEquals("success", result);
        assertEquals(1, attempts.get());
    }

    @Test
    void executeWithRetry_succeedsOnSecondAttempt() {
        AtomicInteger attempts = new AtomicInteger(0);

        String result = RetryUtil.executeWithRetry(() -> {
            if (attempts.incrementAndGet() < 2) {
                throw new RuntimeException("transient error");
            }
            return "success";
        }, 3, 10, "test-op");

        assertEquals("success", result);
        assertEquals(2, attempts.get());
    }

    @Test
    void executeWithRetry_failsAfterMaxRetries() {
        AtomicInteger attempts = new AtomicInteger(0);

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
            RetryUtil.executeWithRetry(() -> {
                attempts.incrementAndGet();
                throw new RuntimeException("persistent error");
            }, 3, 10, "test-op")
        );

        assertEquals(3, attempts.get());
        assertTrue(ex.getMessage().contains("test-op"));
        assertTrue(ex.getMessage().contains("3 attempts"));
    }

    @Test
    void executeWithRetry_runnable_succeedsOnFirstAttempt() {
        AtomicInteger attempts = new AtomicInteger(0);

        RetryUtil.executeWithRetry(() -> attempts.incrementAndGet(), 3, 10, "test-op");

        assertEquals(1, attempts.get());
    }

    @Test
    void executeWithRetry_runnable_failsAfterMaxRetries() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThrows(RuntimeException.class, () ->
            RetryUtil.executeWithRetry((Runnable) () -> {
                attempts.incrementAndGet();
                throw new RuntimeException("persistent error");
            }, 3, 10, "test-op")
        );

        assertEquals(3, attempts.get());
    }
}
