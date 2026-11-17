package com.sparta.logistics.slack_service.infrastructure.config.kafka;

import com.sparta.logistics.slack_service.application.service.SlackService;
import com.sparta.logistics.slack_service.domain.repository.ProcessedEventRepository;
import com.sparta.logistics.slack_service.infrastructure.dto.DeliveryInfoDto;
import com.sparta.logistics.slack_service.infrastructure.slack.SlackPermanentException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

/**
 * 실제 Kafka 브로커(KRaft) + PostgreSQL 로 slack-service 컨슈머의 장애 대응을 검증한다.
 * Slack/AI 등 외부 호출을 하는 SlackService 만 mock.
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class KafkaConsumerIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.1");

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    /** 컨텍스트(컨슈머 그룹) 생성 전에 이미 쌓여 있던 메시지 */
    private static final UUID PRE_START_ORDER_ID = UUID.randomUUID();
    private static final String PRE_START_EVENT_ID = UUID.randomUUID().toString();

    private static KafkaProducer<String, byte[]> producer;
    private static KafkaConsumer<String, byte[]> dltConsumer;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
        registry.add("spring.jpa.show-sql", () -> "false");
        registry.add("EUREKA_URL", () -> "localhost:8761");
        registry.add("eureka.client.enabled", () -> "false");
        registry.add("slack.bot.token", () -> "test-token");
        registry.add("slack.api.url", () -> "http://localhost:1");
        registry.add("kafka.consumer.retry.initial-interval-ms", () -> "200");
        registry.add("kafka.consumer.retry.max-interval-ms", () -> "500");
        registry.add("kafka.consumer.retry.transient.max-retries", () -> "6");
        registry.add("kafka.consumer.retry.transient.initial-interval-ms", () -> "100");
        registry.add("kafka.consumer.retry.transient.max-interval-ms", () -> "200");
    }

    @MockitoBean SlackService slackService;
    @Autowired ProcessedEventRepository processedEventRepository;

    @BeforeAll
    static void setUpClients() {
        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
        // Spring 컨텍스트(=컨슈머 그룹)가 뜨기 전에 발행 → auto.offset.reset=earliest 검증용
        send(KafkaTopics.HUB, "pre-start-event", json(PRE_START_ORDER_ID), Map.of(KafkaTopics.EVENT_ID_HEADER, PRE_START_EVENT_ID));

        dltConsumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class));
        dltConsumer.subscribe(List.of(KafkaTopics.HUB + KafkaTopics.DLT_SUFFIX));
    }

    @AfterAll
    static void closeClients() {
        producer.close();
        dltConsumer.close();
    }

    @Test
    void 컨슈머_그룹_생성_전에_발행된_메시지도_처리한다_earliest() {
        // mock 은 테스트마다 초기화되므로 DB 의 처리완료 기록으로 확인
        await().atMost(Duration.ofSeconds(30)).until(() -> processedEventRepository.existsById(PRE_START_EVENT_ID));
    }

    @Test
    void Outbox_발행_형식_메시지를_역직렬화하여_처리하고_처리완료를_기록한다() {
        UUID orderId = UUID.randomUUID();
        String eventId = UUID.randomUUID().toString();

        send(KafkaTopics.HUB, "delivery-1", json(orderId), Map.of(KafkaTopics.EVENT_ID_HEADER, eventId));

        verify(slackService, timeout(30_000)).processNotification(argThat(dto ->
                orderId.equals(dto.getOrderId())
                        && "U-SLACK".equals(dto.getSlackId())
                        && "서울시".equals(dto.getAddress())
                        && Long.valueOf(7L).equals(dto.getDeliveryManagerId())
                        && dto.getProductId() != null), any());
        await().atMost(Duration.ofSeconds(10)).until(() -> processedEventRepository.existsById(eventId));
    }

    @Test
    void 같은_eventId_가_중복_수신되어도_한번만_처리한다() {
        UUID orderId = UUID.randomUUID();
        UUID sentinel = UUID.randomUUID();
        String eventId = UUID.randomUUID().toString();
        String key = "delivery-dup"; // 같은 key → 같은 파티션 → 순서 보장

        send(KafkaTopics.HUB, key, json(orderId), Map.of(KafkaTopics.EVENT_ID_HEADER, eventId));
        send(KafkaTopics.HUB, key, json(orderId), Map.of(KafkaTopics.EVENT_ID_HEADER, eventId));
        send(KafkaTopics.HUB, key, json(sentinel), Map.of(KafkaTopics.EVENT_ID_HEADER, UUID.randomUUID().toString()));

        // sentinel 이 처리됐다면 앞의 중복 메시지도 모두 처리(skip)된 상태
        verify(slackService, timeout(30_000)).processNotification(orderIdIs(sentinel), any());
        verify(slackService, times(1)).processNotification(orderIdIs(orderId), any());
    }

    @Test
    void 이전_형식_메시지_TypeId_헤더_eventId_없음_도_처리한다() {
        UUID orderId = UUID.randomUUID();

        send(KafkaTopics.HUB, null, json(orderId), Map.of("__TypeId__",
                "com.sparta.logistics.delivery_service.infrastructure.messaging.dto.DeliveryInfoDto"));

        verify(slackService, timeout(30_000)).processNotification(orderIdIs(orderId), any());
    }

    @Test
    void COMPANY_토픽도_처리한다() {
        UUID orderId = UUID.randomUUID();

        send(KafkaTopics.COMPANY, "company-1", json(orderId), Map.of(KafkaTopics.EVENT_ID_HEADER, UUID.randomUUID().toString()));

        verify(slackService, timeout(30_000)).processNotification(orderIdIs(orderId), any());
    }

    @Test
    void 처리_실패시_백오프_재시도후_DLT_로_격리하고_다음_메시지는_계속_처리한다() {
        UUID failingOrderId = UUID.randomUUID();
        UUID nextOrderId = UUID.randomUUID();
        String eventId = UUID.randomUUID().toString();
        doThrow(new RuntimeException("hub-service 장애")).when(slackService).processNotification(orderIdIs(failingOrderId), any());

        send(KafkaTopics.HUB, "delivery-fail", json(failingOrderId), Map.of(KafkaTopics.EVENT_ID_HEADER, eventId));
        send(KafkaTopics.HUB, "delivery-fail", json(nextOrderId), Map.of(KafkaTopics.EVENT_ID_HEADER, UUID.randomUUID().toString()));

        // 최초 1회 + 재시도 3회 후 다음 메시지로 진행 (컨슈머가 막히지 않음)
        verify(slackService, timeout(30_000)).processNotification(orderIdIs(nextOrderId), any());
        verify(slackService, times(4)).processNotification(orderIdIs(failingOrderId), any());

        ConsumerRecord<String, byte[]> dlt = pollDlt(r -> header(r, KafkaTopics.EVENT_ID_HEADER).equals(eventId));
        assertThat(new String(dlt.value(), StandardCharsets.UTF_8)).contains(failingOrderId.toString());
        assertThat(header(dlt, "kafka_dlt-original-topic")).isEqualTo(KafkaTopics.HUB);
        assertThat(header(dlt, "kafka_dlt-exception-message")).contains("hub-service 장애");
        // 실패한 이벤트는 처리완료로 기록되지 않아 DLT 재처리 시 다시 처리 가능
        assertThat(processedEventRepository.existsById(eventId)).isFalse();
    }

    @Test
    void 내부_서비스_연결_실패는_더_오래_재시도해서_복구되면_DLT_없이_처리한다() {
        UUID orderId = UUID.randomUUID();
        String eventId = UUID.randomUUID().toString();
        RuntimeException down = new RuntimeException("order-service 재시작 중",
                new java.net.ConnectException("Connection refused"));
        // 기본 재시도(3회)보다 많은 5번 연속 실패 후 복구
        doThrow(down).doThrow(down).doThrow(down).doThrow(down).doThrow(down).doNothing()
                .when(slackService).processNotification(orderIdIs(orderId), any());

        send(KafkaTopics.HUB, "delivery-transient", json(orderId), Map.of(KafkaTopics.EVENT_ID_HEADER, eventId));

        verify(slackService, timeout(30_000).times(6)).processNotification(orderIdIs(orderId), any());
        await().atMost(Duration.ofSeconds(10)).until(() -> processedEventRepository.existsById(eventId));
    }

    @Test
    void Slack_설정_오류같은_영구_실패는_재시도_없이_바로_DLT_로_보낸다() {
        UUID orderId = UUID.randomUUID();
        UUID nextOrderId = UUID.randomUUID();
        String eventId = UUID.randomUUID().toString();
        doThrow(new SlackPermanentException("Slack 발송 거부: channel_not_found"))
                .when(slackService).processNotification(orderIdIs(orderId), any());

        send(KafkaTopics.HUB, "delivery-perm", json(orderId), Map.of(KafkaTopics.EVENT_ID_HEADER, eventId));
        send(KafkaTopics.HUB, "delivery-perm", json(nextOrderId), Map.of(KafkaTopics.EVENT_ID_HEADER, UUID.randomUUID().toString()));

        verify(slackService, timeout(30_000)).processNotification(orderIdIs(nextOrderId), any());
        verify(slackService, times(1)).processNotification(orderIdIs(orderId), any()); // 재시도 없음

        ConsumerRecord<String, byte[]> dlt = pollDlt(r -> header(r, KafkaTopics.EVENT_ID_HEADER).equals(eventId));
        assertThat(header(dlt, "kafka_dlt-exception-message")).contains("channel_not_found");
        assertThat(processedEventRepository.existsById(eventId)).isFalse();
    }

    @Test
    void 역직렬화_불가_메시지는_재시도없이_원본_그대로_DLT_로_보내고_컨슈머는_계속_동작한다() {
        byte[] poison = "{not-json!!".getBytes(StandardCharsets.UTF_8);
        String poisonEventId = UUID.randomUUID().toString();
        UUID nextOrderId = UUID.randomUUID();

        send(KafkaTopics.HUB, "delivery-poison", poison, Map.of(KafkaTopics.EVENT_ID_HEADER, poisonEventId));
        send(KafkaTopics.HUB, "delivery-poison", json(nextOrderId), Map.of(KafkaTopics.EVENT_ID_HEADER, UUID.randomUUID().toString()));

        verify(slackService, timeout(30_000)).processNotification(orderIdIs(nextOrderId), any());

        ConsumerRecord<String, byte[]> dlt = pollDlt(r -> header(r, KafkaTopics.EVENT_ID_HEADER).equals(poisonEventId));
        assertThat(dlt.value()).isEqualTo(poison);
        assertThat(header(dlt, "kafka_dlt-exception-fqcn")).contains("DeserializationException");
    }

    @Test
    void DLT_토픽이_자동_생성된다() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                    assertThat(admin.listTopics().names().get())
                            .contains(KafkaTopics.HUB + KafkaTopics.DLT_SUFFIX, KafkaTopics.COMPANY + KafkaTopics.DLT_SUFFIX));
        }
    }

    // ---------------------------------------------------------------------------------------------

    private static DeliveryInfoDto orderIdIs(UUID orderId) {
        return argThat(dto -> dto != null && orderId.equals(dto.getOrderId()));
    }

    /** delivery-service Outbox 가 발행하는 것과 동일한 JSON 형식 */
    private static byte[] json(UUID orderId) {
        return ("""
                {"departureHubId":"%s","destinationHubId":"%s","deliveryManagerId":7,"productId":"%s",\
                "orderId":"%s","slackId":"U-SLACK","address":"서울시"}"""
                .formatted(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), orderId))
                .getBytes(StandardCharsets.UTF_8);
    }

    private static void send(String topic, String key, byte[] value, Map<String, String> headers) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);
        headers.forEach((k, v) -> record.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
        try {
            producer.send(record).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static synchronized ConsumerRecord<String, byte[]> pollDlt(Predicate<ConsumerRecord<String, byte[]>> matcher) {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            for (ConsumerRecord<String, byte[]> r : dltConsumer.poll(Duration.ofMillis(500))) {
                if (matcher.test(r)) return r;
            }
        }
        throw new AssertionError("DLT 에서 메시지를 찾지 못함");
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        var h = record.headers().lastHeader(name);
        return h == null ? "" : new String(h.value(), StandardCharsets.UTF_8);
    }
}
