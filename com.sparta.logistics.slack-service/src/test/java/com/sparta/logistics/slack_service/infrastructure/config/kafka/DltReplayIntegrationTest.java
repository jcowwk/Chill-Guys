package com.sparta.logistics.slack_service.infrastructure.config.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sparta.logistics.slack_service.application.dto.response.DltReplayResponseDto;
import com.sparta.logistics.slack_service.application.dto.response.DltReplayResponseDto.Action;
import com.sparta.logistics.slack_service.application.service.DltReplayService;
import com.sparta.logistics.slack_service.application.service.ProcessedEventCleaner;
import com.sparta.logistics.slack_service.application.service.SlackService;
import com.sparta.logistics.slack_service.domain.entity.ProcessedEvent;
import com.sparta.logistics.slack_service.domain.repository.ProcessedEventRepository;
import com.sparta.logistics.slack_service.infrastructure.dto.DeliveryInfoDto;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * DLT 재처리 도구와 처리 이력 정리를 실제 Kafka + PostgreSQL 로 검증한다.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DltReplayIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.1");

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

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
        registry.add("kafka.consumer.retry.initial-interval-ms", () -> "100");
        registry.add("kafka.consumer.retry.max-interval-ms", () -> "200");
    }

    @MockitoBean SlackService slackService;
    @Autowired DltReplayService dltReplayService;
    @Autowired ProcessedEventRepository processedEventRepository;
    @Autowired ProcessedEventCleaner processedEventCleaner;
    @Autowired MockMvc mockMvc;
    @Autowired ObjectMapper objectMapper;

    private static KafkaProducer<String, byte[]> producer;

    @BeforeAll
    static void setUpProducer() {
        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
    }

    @AfterAll
    static void closeProducer() {
        producer.close();
    }

    @Test
    @Order(1)
    void 권한이_없으면_403_잘못된_토픽은_400() throws Exception {
        mockMvc.perform(get("/api/master/slack/dlt")).andExpect(status().isForbidden());
        mockMvc.perform(post("/api/master/slack/dlt/replay").header("X-User-Role", "ROLE_USER"))
                .andExpect(status().isForbidden());
        mockMvc.perform(post("/api/master/slack/dlt/replay").header("X-User-Role", "ROLE_MASTER")
                        .param("topic", "ORDER"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @Order(2)
    void 실패해서_DLT_로_간_메시지를_원인_해결후_재처리하면_알림이_처리되고_다시_재투입되지_않는다() throws Exception {
        UUID orderId = UUID.randomUUID();
        String eventId = UUID.randomUUID().toString();
        when(slackService.saveSlackMessage(orderIdIs(orderId))).thenThrow(new RuntimeException("hub-service 장애"));

        send(eventId, json(orderId));
        // 조회는 몇 번을 해도 커밋하지 않으므로 같은 메시지가 보인다
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(find(dltReplayService.peek("HUB", 100), eventId))
                        .hasValueSatisfying(i -> assertThat(i.action()).isEqualTo(Action.WOULD_REPLAY)));
        assertThat(find(dltReplayService.peek("HUB", 100), eventId)).isPresent();

        // 원인 해결 (hub-service 복구)
        Mockito.reset(slackService);

        String body = mockMvc.perform(post("/api/master/slack/dlt/replay")
                        .header("X-User-Role", "ROLE_MASTER").param("topic", "HUB").param("max", "100"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString(StandardCharsets.UTF_8);
        JsonNode result = objectMapper.readTree(body);
        assertThat(result.get("replayed").asInt()).isGreaterThanOrEqualTo(1);

        verify(slackService, timeout(20_000)).saveSlackMessage(orderIdIs(orderId));
        await().atMost(Duration.ofSeconds(10)).until(() -> processedEventRepository.existsById(eventId));

        // 오프셋이 커밋되어 같은 메시지를 두 번 재투입하지 않는다
        assertThat(find(dltReplayService.peek("HUB", 100), eventId)).isEmpty();
    }

    @Test
    @Order(3)
    void 원인이_해결되지_않은채_반복_재처리하면_한도에서_멈춘다() {
        UUID orderId = UUID.randomUUID();
        String eventId = UUID.randomUUID().toString();
        when(slackService.saveSlackMessage(orderIdIs(orderId))).thenThrow(new RuntimeException("여전히 장애"));

        send(eventId, json(orderId));

        // 재처리 → 다시 실패 → DLT (replay-count 헤더가 유지되며 증가)
        for (int expectedCount = 0; expectedCount < 3; expectedCount++) {
            int count = expectedCount;
            DltReplayResponseDto.Item item = awaitInDlt(eventId);
            assertThat(dltReplayService.peek("HUB", 100).messages().stream()
                    .filter(i -> eventId.equals(i.eventId())).count())
                    .as("DLT 에 같은 메시지는 한 건만 있어야 함 (한 실행에서 중복 재투입 금지)").isEqualTo(1);
            assertThat(item.replayCount()).isEqualTo(count);
            assertThat(item.lastError()).contains("여전히 장애");
            DltReplayResponseDto replay = dltReplayService.replay("HUB", 100);
            assertThat(find(replay, eventId)).hasValueSatisfying(i -> assertThat(i.action()).isEqualTo(Action.REPLAYED));
        }

        // 3번 재처리 후 다시 DLT 에 오면 더 이상 재투입하지 않음
        DltReplayResponseDto.Item last = awaitInDlt(eventId);
        assertThat(last.replayCount()).isEqualTo(3);
        DltReplayResponseDto replay = dltReplayService.replay("HUB", 100);
        assertThat(find(replay, eventId)).hasValueSatisfying(i -> {
            assertThat(i.action()).isEqualTo(Action.SKIPPED_LIMIT);
            assertThat(i.reason()).contains("재처리 3회 초과");
        });
        // 최초 1회 + 재처리 3회, 각 회차마다 1 + 재시도 3 = 16번 시도
        verify(slackService, timeout(5_000).times(16)).saveSlackMessage(orderIdIs(orderId));
    }

    @Test
    @Order(4)
    void 역직렬화_불가_메시지는_재투입하지_않고_사유를_알려준다() {
        String eventId = UUID.randomUUID().toString();

        send(eventId, "{broken-json".getBytes(StandardCharsets.UTF_8));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(find(dltReplayService.peek("HUB", 100), eventId))
                        .hasValueSatisfying(i -> assertThat(i.action()).isEqualTo(Action.SKIPPED_POISON)));
        DltReplayResponseDto replay = dltReplayService.replay("HUB", 100);
        assertThat(find(replay, eventId)).hasValueSatisfying(i -> {
            assertThat(i.action()).isEqualTo(Action.SKIPPED_POISON);
            assertThat(i.reason()).contains("역직렬화 불가");
        });
        // 제외된 메시지도 오프셋은 커밋되어 다음 조회에 다시 나오지 않는다 (DLT 토픽에는 보관 기간 동안 남아 있음)
        assertThat(find(dltReplayService.peek("HUB", 100), eventId)).isEmpty();
    }

    @Test
    @Order(5)
    void 처리_이력은_보관기간이_지나면_정리된다() {
        LocalDateTime now = LocalDateTime.now();
        processedEventRepository.save(new ProcessedEvent("old-event", "HUB", now.minusDays(8)));
        processedEventRepository.save(new ProcessedEvent("recent-event", "HUB", now.minusDays(6)));

        int deleted = processedEventCleaner.purge(now);

        assertThat(deleted).isGreaterThanOrEqualTo(1);
        assertThat(processedEventRepository.existsById("old-event")).isFalse();
        assertThat(processedEventRepository.existsById("recent-event")).isTrue();
    }

    // ---------------------------------------------------------------------------------------------

    private DltReplayResponseDto.Item awaitInDlt(String eventId) {
        return await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .until(() -> find(dltReplayService.peek("HUB", 100), eventId), Optional::isPresent)
                .orElseThrow();
    }

    private static Optional<DltReplayResponseDto.Item> find(DltReplayResponseDto result, String eventId) {
        return result.messages().stream().filter(i -> eventId.equals(i.eventId())).findFirst();
    }

    private static DeliveryInfoDto orderIdIs(UUID orderId) {
        return argThat(dto -> dto != null && orderId.equals(dto.getOrderId()));
    }

    private static void send(String eventId, byte[] value) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(KafkaTopics.HUB, "delivery-" + eventId, value);
        record.headers().add(KafkaTopics.EVENT_ID_HEADER, eventId.getBytes(StandardCharsets.UTF_8));
        try {
            producer.send(record).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] json(UUID orderId) {
        return ("""
                {"departureHubId":"%s","destinationHubId":"%s","deliveryManagerId":7,"productId":"%s",\
                "orderId":"%s","slackId":"U-SLACK","address":"서울시"}"""
                .formatted(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), orderId))
                .getBytes(StandardCharsets.UTF_8);
    }
}
