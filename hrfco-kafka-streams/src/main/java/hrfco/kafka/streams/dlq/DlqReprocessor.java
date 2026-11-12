package hrfco.kafka.streams.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import hrfco.kafka.streams.service.StorageResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * DLQ 재처리 컨슈머
 *
 * - 저장 실패로 DLQ에 온 레코드를 세 저장소 모두에 다시 저장한다.
 *   저장이 멱등(관측소 코드 + 관측 시각 키)이라 이미 성공한 저장소에 다시 써도 중복이 생기지 않는다.
 * - 처리 실패(파싱 실패 등)와 봉투를 읽을 수 없는 레코드는 다시 해도 실패하므로 로그만 남기고 넘긴다.
 * - 다시 저장해도 실패하면 오프셋을 커밋하지 않고 그 레코드로 되감은 뒤 backoff만큼 기다렸다 다시 시도한다.
 *   저장소가 회복될 때까지 DLQ 소비를 멈추는 방식이라 레코드를 버리지 않고 순서도 유지한다.
 */
public class DlqReprocessor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(DlqReprocessor.class);

    /** 원본 메시지 값을 받아 다시 저장하고 결과를 돌려준다. 예외는 다시 해도 실패할 데이터 문제로 본다 */
    @FunctionalInterface
    public interface Restorer {
        StorageResult restore(String originalValue) throws Exception;
    }

    private final Consumer<String, String> consumer;
    private final String topic;
    private final Restorer restorer;
    private final Duration backoff;
    private final ObjectMapper mapper = new ObjectMapper();
    private volatile boolean running = true;

    public DlqReprocessor(Consumer<String, String> consumer, String topic, Restorer restorer, Duration backoff) {
        this.consumer = consumer;
        this.topic = topic;
        this.restorer = restorer;
        this.backoff = backoff;
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(List.of(topic));
            logger.info("DLQ reprocessor started. Topic: {}, backoff: {}", topic, backoff);
            while (running) {
                step();
            }
        } catch (WakeupException e) {
            if (running) {
                throw e;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            consumer.close();
            logger.info("DLQ reprocessor stopped");
        }
    }

    public void shutdown() {
        running = false;
        consumer.wakeup();
    }

    /**
     * 루프 한 바퀴. 브로커 재시작 중 poll·commit 타임아웃 같은 Kafka 오류로 재처리 스레드가 죽지 않도록
     * 로그를 남기고 backoff 뒤 계속한다. 커밋에 실패한 레코드는 다시 저장되지만 저장이 멱등이라 괜찮다.
     */
    void step() throws InterruptedException {
        try {
            pollOnce();
        } catch (WakeupException e) {
            throw e;
        } catch (KafkaException e) {
            logger.error("DLQ reprocessor Kafka error, retry after {}", backoff, e);
            Thread.sleep(backoff.toMillis());
        }
    }

    /**
     * 한 번 poll한 레코드를 처리한다. 파티션마다 실패한 레코드에서 멈춘다.
     */
    void pollOnce() throws InterruptedException {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (TopicPartition tp : records.partitions()) {
            for (ConsumerRecord<String, String> record : records.records(tp)) {
                if (!handle(record)) {
                    consumer.seek(tp, record.offset());
                    Thread.sleep(backoff.toMillis());
                    break;
                }
                consumer.commitSync(Map.of(tp, new OffsetAndMetadata(record.offset() + 1)));
            }
        }
    }

    /**
     * @return true면 끝난 레코드(성공 또는 넘김), false면 나중에 다시 시도할 레코드
     */
    private boolean handle(ConsumerRecord<String, String> record) {
        DlqEnvelope envelope;
        try {
            envelope = DlqEnvelope.parse(mapper, record.value());
        } catch (Exception e) {
            logger.error("Unreadable DLQ envelope, skipped. offset={}, value={}", record.offset(), record.value(), e);
            return true;
        }

        if (!envelope.isStorageFailure()) {
            logger.error("Not a storage failure, skipped. offset={}, reason={}, value={}",
                record.offset(), envelope.errorReason(), envelope.originalValue());
            return true;
        }

        StorageResult result;
        try {
            result = restorer.restore(envelope.originalValue());
        } catch (Exception e) {
            logger.error("Restore failed with invalid data, skipped. offset={}, value={}",
                record.offset(), envelope.originalValue(), e);
            return true;
        }

        if (result.isAllSuccess()) {
            logger.info("DLQ record restored to all storages. offset={}, key={}", record.offset(), envelope.originalKey());
            return true;
        }

        logger.warn("DLQ record still failing, will retry after {}. offset={}, {}", backoff, record.offset(), result);
        return false;
    }
}
