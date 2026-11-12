// 한강 홍수통제소 수위 데이터 스트림 처리 애플리케이션
// NiFi에서 PublishKafka로 전송된 hrfco-waterlevel 토픽의 데이터 처리
// 수위 이상치 탐지, 홍수 경보 수준 분석, MinIO/TimescaleDB/MongoDB 저장
package hrfco.kafka.streams;

import hrfco.kafka.streams.cache.StationThresholdCache;
import hrfco.kafka.streams.dlq.DeadLetterQueueProducer;
import hrfco.kafka.streams.dlq.DlqReprocessor;
import hrfco.kafka.streams.processor.AlertPublisher;
import hrfco.kafka.streams.processor.AnomalyDetector;
import hrfco.kafka.streams.processor.WaterLevelProcessor;
import hrfco.kafka.streams.repository.MinIORepository;
import hrfco.kafka.streams.repository.MongoDBRepository;
import hrfco.kafka.streams.repository.TimescaleDBRepository;
import hrfco.kafka.streams.service.HRFCOStorageService;
import hrfco.kafka.streams.util.StationThresholdLoader;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * HRFCO Kafka Streams 애플리케이션 부트스트랩
 * 컴포넌트 초기화 및 Kafka Streams 토폴로지 구성
 */
public class HRFCOStreamsApp {
    private static final Logger logger = LoggerFactory.getLogger(HRFCOStreamsApp.class);

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");

        // Repository 초기화
        TimescaleDBRepository timescaleDB;
        MongoDBRepository mongoDB;
        HRFCOStorageService storageService;
        WaterLevelProcessor processor;
        AlertPublisher alertPublisher;
        DeadLetterQueueProducer dlqProducer;

        try {
            timescaleDB = new TimescaleDBRepository();
            MinIORepository minIO = new MinIORepository();
            mongoDB = new MongoDBRepository();

            storageService = new HRFCOStorageService(timescaleDB, minIO, mongoDB);

            // 프로세서 컴포넌트 초기화
            AnomalyDetector anomalyDetector = new AnomalyDetector(mongoDB);
            alertPublisher = new AlertPublisher(bootstrapServers);
            dlqProducer = new DeadLetterQueueProducer(bootstrapServers);

            processor = new WaterLevelProcessor(
                new ObjectMapper(), anomalyDetector, storageService, alertPublisher, dlqProducer);

            logger.info("All components initialized successfully");

            // 관측소 임계값 로드
            loadStationThresholds(mongoDB);

        } catch (Exception e) {
            logger.error("Failed to initialize services", e);
            System.exit(1);
            return;
        }

        // Kafka Streams 설정
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "hrfco-streams-processor");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        String numThreadsStr = System.getenv("NUM_STREAM_THREADS");
        int numThreads = numThreadsStr != null ? Integer.parseInt(numThreadsStr)
                : Math.max(1, Runtime.getRuntime().availableProcessors());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThreads);
        logger.info("Kafka Streams configured with {} threads", numThreads);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 토폴로지 구성
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> waterLevelStream = builder.stream("hrfco-waterlevel");
        waterLevelStream.foreach(processor::process);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // DLQ 재처리 컨슈머 (저장 실패 레코드를 세 저장소에 다시 저장)
        DlqReprocessor dlqReprocessor = startDlqReprocessor(bootstrapServers, processor);

        // 종료 훅
        final TimescaleDBRepository tsdb = timescaleDB;
        final MongoDBRepository mdb = mongoDB;
        final AlertPublisher ap = alertPublisher;
        final DeadLetterQueueProducer dlq = dlqProducer;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down HRFCO Kafka Streams application...");
            streams.close();
            if (dlqReprocessor != null) {
                dlqReprocessor.shutdown();
            }
            storageService.shutdown();
            tsdb.close();
            mdb.close();
            ap.close();
            dlq.close();
        }));

        // 시작
        logger.info("Starting HRFCO Kafka Streams application...");
        streams.start();
        logger.info("HRFCO Kafka Streams is running. Topic: hrfco-waterlevel");
    }

    /**
     * DLQ 재처리 컨슈머를 별도 스레드로 시작한다. DLQ_REPROCESS_ENABLED=false면 시작하지 않는다.
     */
    private static DlqReprocessor startDlqReprocessor(String bootstrapServers, WaterLevelProcessor processor) {
        if ("false".equalsIgnoreCase(System.getenv("DLQ_REPROCESS_ENABLED"))) {
            logger.info("DLQ reprocessor disabled");
            return null;
        }
        long backoffMs = Long.parseLong(System.getenv().getOrDefault("DLQ_REPROCESS_BACKOFF_MS", "10000"));

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "hrfco-dlq-reprocessor");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);   // 다시 저장에 성공한 뒤에만 커밋한다

        DlqReprocessor reprocessor = new DlqReprocessor(
            new KafkaConsumer<>(props),
            DeadLetterQueueProducer.DLQ_TOPIC,
            processor::restore,
            Duration.ofMillis(backoffMs));

        Thread thread = new Thread(reprocessor, "dlq-reprocessor");
        thread.setDaemon(true);
        thread.start();
        return reprocessor;
    }

    private static void loadStationThresholds(MongoDBRepository mongoDB) {
        try {
            logger.info("Loading station thresholds from HRFCO API...");
            StationThresholdCache.clear();

            StationThresholdLoader loader = new StationThresholdLoader(mongoDB);
            loader.loadStationThresholds();

            logger.info("Station thresholds loaded. Cached: {}", StationThresholdCache.size());
        } catch (Exception e) {
            logger.error("Failed to load station thresholds", e);
        }
    }
}
