package com.magpie.hrfco_monitoring_server.config;

import com.magpie.hrfco_monitoring_server.service.AlertService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerConfig {
    
    @Value("${spring.kafka.bootstrap-servers:kafka:9092}")
    private String bootstrapServers;
    
    @Value("${kafka.alert.topic:hrfco-alerts}")
    private String alertTopic;
    
    private final AlertService alertService;
    
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "alert-server-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        
        return new DefaultKafkaConsumerFactory<>(props);
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
    
    /**
     * Kafka에서 알림 수신 및 브로드캐스트
     */
    @KafkaListener(topics = "${kafka.alert.topic:hrfco-alerts}", groupId = "alert-server-consumer")
    public void consumeAlert(String alertJson, org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record) {
        String stationCode = record.key(); // 관측소 코드
        
        log.info("Received alert from Kafka - Station: {}, Alert: {}", stationCode, alertJson);
        
        // SSE로 브로드캐스트
        alertService.broadcastAlert(stationCode, alertJson);
    }
}



