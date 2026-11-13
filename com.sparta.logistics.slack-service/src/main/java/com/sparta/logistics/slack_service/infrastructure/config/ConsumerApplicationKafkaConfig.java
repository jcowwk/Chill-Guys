package com.sparta.logistics.slack_service.infrastructure.config;

import com.sparta.logistics.slack_service.infrastructure.config.kafka.KafkaTopics;
import com.sparta.logistics.slack_service.infrastructure.dto.DeliveryInfoDto;
import com.sparta.logistics.slack_service.infrastructure.slack.SlackPermanentException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@EnableKafka
@Configuration
public class ConsumerApplicationKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /** 리스너당 컨슈머 스레드 수. 토픽 파티션 수 이하로 (초과분은 놀게 됨) */
    @Value("${kafka.consumer.concurrency:3}")
    private int concurrency;

    @Value("${kafka.consumer.max-poll-records:10}")
    private int maxPollRecords;

    @Value("${kafka.consumer.retry.max-retries:3}")
    private int maxRetries;

    @Value("${kafka.consumer.retry.initial-interval-ms:1000}")
    private long initialIntervalMs;

    @Value("${kafka.consumer.retry.max-interval-ms:10000}")
    private long maxIntervalMs;

    @Bean
    public ConsumerFactory<String, DeliveryInfoDto> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // 역직렬화 실패(poison pill)가 컨슈머를 무한 루프에 빠뜨리지 않도록 ErrorHandlingDeserializer 로 감싼다
        // (기존에는 delegate 설정만 있고 실제 deserializer 는 JsonDeserializer 여서 동작하지 않았음)
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        // 프로듀서 패키지명(__TypeId__ 헤더)에 의존하지 않고 항상 로컬 DTO 로 역직렬화
        configProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        configProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, DeliveryInfoDto.class.getName());

        // 오프셋은 처리 완료 후 컨테이너가 커밋 (처리 전 커밋으로 인한 유실 방지)
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // 컨슈머 그룹이 처음 생성될 때 그 전에 쌓인 메시지를 놓치지 않도록
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 한 번 poll 한 레코드를 max.poll.interval.ms(5분) 안에 다 처리해야 그룹에서 쫓겨나지 않는다.
        // 알림 1건이 외부 API 5회 호출(+재시도)이라 느려질 수 있으므로 기본값 500 대신 작게 가져온다.
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);

        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, DeliveryInfoDto> kafkaListenerContainerFactory(
            DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, DeliveryInfoDto> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // 파티션별 병렬 처리 (부하 테스트: 1 스레드 12.8건/s → 3 스레드 37.2건/s)
        factory.setConcurrency(concurrency);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        // 레코드 단위 커밋: 장애 시 재처리 범위를 최소화
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }

    /**
     * 처리 실패 시 지수 백오프로 재시도하고, 그래도 실패하면 DLT(<topic>.DLT)로 보내 유실 없이 격리한다.
     * (기본 설정은 백오프 없이 10회 재시도 후 로그만 남기고 메시지를 버림)
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> deadLetterKafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(deadLetterKafkaTemplate,
                // partition -1: DLT 파티션 수가 원본과 달라도 발행 가능하도록 파티션 지정을 Kafka 에 맡김
                (record, ex) -> {
                    log.error("[Kafka] 재시도 초과 → DLT 이동 topic={}, partition={}, offset={}, key={}, error={}",
                            record.topic(), record.partition(), record.offset(), record.key(), ex.toString());
                    return new TopicPartition(record.topic() + KafkaTopics.DLT_SUFFIX, -1);
                });

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backOff.setInitialInterval(initialIntervalMs);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(maxIntervalMs);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        // 다시 보내도 성공하지 않는 Slack 오류(채널 없음, 토큰 오류 등)는 재시도 없이 바로 DLT 로
        errorHandler.addNotRetryableExceptions(SlackPermanentException.class);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("[Kafka] 메시지 처리 실패, 재시도 {}회차 topic={}, offset={}, error={}",
                        deliveryAttempt, record.topic(), record.offset(), ex.toString()));
        return errorHandler;
    }

    /**
     * DLT 발행용 프로듀서. 역직렬화 실패 메시지는 원본 byte[] 그대로, 정상 객체는 JSON 으로 보낸다.
     */
    @Bean
    public KafkaTemplate<Object, Object> deadLetterKafkaTemplate() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        DefaultKafkaProducerFactory<Object, Object> producerFactory = new DefaultKafkaProducerFactory<>(
                configProps, bytesStringOrJsonSerializer(), bytesStringOrJsonSerializer());
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public NewTopic hubDeadLetterTopic() {
        return TopicBuilder.name(KafkaTopics.HUB + KafkaTopics.DLT_SUFFIX).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic companyDeadLetterTopic() {
        return TopicBuilder.name(KafkaTopics.COMPANY + KafkaTopics.DLT_SUFFIX).partitions(1).replicas(1).build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Serializer<Object> bytesStringOrJsonSerializer() {
        // 순서 중요: 앞에서부터 타입 매칭 (byte[] → String → 그 외 객체)
        Map<Class<?>, Serializer<?>> delegates = new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(String.class, new StringSerializer());
        delegates.put(Object.class, new JsonSerializer<>());
        return (Serializer) new DelegatingByTypeSerializer(delegates, true);
    }
}
