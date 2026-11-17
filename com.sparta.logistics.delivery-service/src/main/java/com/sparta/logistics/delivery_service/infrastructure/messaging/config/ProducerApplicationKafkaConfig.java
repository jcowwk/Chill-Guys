package com.sparta.logistics.delivery_service.infrastructure.messaging.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ProducerApplicationKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topic.partitions:3}")
    private int partitions;

    /** 브로커 1대 구성이라 복제본 1개. 브로커 수보다 크면 토픽 생성이 실패한다. */
    @Value("${kafka.topic.replicas:1}")
    private short replicas;

    /** 복제본 수 이하여야 한다. 더 크면 acks=all 발행이 NotEnoughReplicas 로 계속 실패한다. */
    @Value("${kafka.topic.min-insync-replicas:1}")
    private int minInsyncReplicas;

    /**
     * Outbox payload 는 이미 JSON 문자열이므로 String 으로 그대로 발행한다.
     * (컨슈머는 타입 헤더 없이 기본 타입으로 역직렬화 → 서비스 간 패키지명 결합 제거)
     */
    @Bean
    public ProducerFactory<String, String> outboxProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 브로커 1대 구성: 리더(=유일한 복제본)가 디스크에 기록하면 응답한다.
        // 멱등 프로듀서는 acks=all 이 필요하고, 복제본이 1개라 acks=1 과 같은 지연으로 동작한다.
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // 브로커 다운 시 send() 가 기본 60초 동안 블로킹되지 않도록 빠르게 실패 → Outbox 가 재시도 담당
        configProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);

        // 비동기 배치 전송 시 여러 레코드를 한 요청으로 묶어 처리량 향상
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> outboxKafkaTemplate() {
        return new KafkaTemplate<>(outboxProducerFactory());
    }

    /**
     * 토픽은 발행 주체(delivery-service)가 선언한다.
     * 파티션 수 = 컨슈머 병렬 처리의 상한 (auto-create 기본값 1 이면 컨슈머를 늘려도 1개 스레드만 일함).
     * 이미 존재하는 토픽은 파티션이 더 적을 때만 늘린다 (줄이지는 않음).
     */
    private NewTopic topic(String name) {
        return TopicBuilder.name(name)
                .partitions(partitions)
                .replicas(replicas)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(minInsyncReplicas))
                .build();
    }

    @Bean
    public NewTopic hubTopic() {
        return topic("HUB");
    }

    @Bean
    public NewTopic companyTopic() {
        return topic("COMPANY");
    }
}
