package com.sparta.logistics.delivery_service.infrastructure.messaging.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sparta.logistics.delivery_service.application.dto.response.DeliveryManagerInfoDto;
import com.sparta.logistics.delivery_service.application.service.DeliveryService;
import com.sparta.logistics.delivery_service.domain.model.*;
import com.sparta.logistics.delivery_service.domain.repository.DeliveryRepository;
import com.sparta.logistics.delivery_service.domain.repository.OutboxEventRepository;
import com.sparta.logistics.delivery_service.infrastructure.messaging.dto.DeliveryInfoDto;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 실제 Kafka 브로커(KRaft) + PostgreSQL 로 Outbox 발행/장애 복구를 검증한다.
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OutboxKafkaIntegrationTest extends OutboxIntegrationTestSupport {

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.1");

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registerCommonProperties(registry, postgres);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired DeliveryService deliveryService;
    @Autowired DeliveryRepository deliveryRepository;
    @Autowired OutboxEventRepository outboxEventRepository;
    @Autowired OutboxEventRecorder outboxEventRecorder;
    @Autowired OutboxPublisher outboxPublisher;
    @Autowired OutboxRelay outboxRelay;
    @Autowired TransactionTemplate transactionTemplate;
    @Autowired ObjectMapper objectMapper;

    private static KafkaConsumer<String, String> hubConsumer;

    @BeforeAll
    static void createConsumer() {
        hubConsumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "outbox-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        hubConsumer.subscribe(List.of("HUB"));
    }

    @AfterAll
    static void closeConsumer() {
        hubConsumer.close();
    }

    @AfterEach
    void cleanUp() {
        outboxEventRepository.deleteAll();
        deliveryRepository.deleteAll();
    }

    @Test
    @Order(1)
    void 배송_배정시_Outbox_에_기록되고_릴레이가_Kafka_로_발행한다() throws Exception {
        Delivery delivery = deliveryRepository.save(pendingDelivery());
        DeliveryManagerInfoDto manager = mock(DeliveryManagerInfoDto.class);
        when(manager.getId()).thenReturn(7L);
        when(manager.getSlackId()).thenReturn("U-SLACK-7");
        when(deliveryManagerClient.assignDeliveryManager(any(), any(), eq("HUB"))).thenReturn(manager);

        deliveryService.assignPendingDeliveries();

        // 같은 트랜잭션으로 배송 상태 변경 + Outbox 기록
        OutboxEvent event = single(outboxEventRepository.findAll());
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(deliveryRepository.findById(delivery.getId()).orElseThrow().getDeliveryStatus())
                .isEqualTo(DeliveryStatus.SHIPPING);

        outboxRelay.relay();

        assertThat(outboxEventRepository.findById(event.getId()).orElseThrow().getStatus())
                .isEqualTo(OutboxStatus.PUBLISHED);

        ConsumerRecord<String, String> record = pollOne(event.getId());
        assertThat(record.key()).isEqualTo(delivery.getId().toString());
        assertThat(record.headers().lastHeader("__TypeId__")).isNull();

        JsonNode json = objectMapper.readTree(record.value());
        assertThat(json.get("orderId").asText()).isEqualTo(delivery.getOrderId().toString());
        assertThat(json.get("slackId").asText()).isEqualTo("U-SLACK-7");
        assertThat(json.get("deliveryManagerId").asLong()).isEqualTo(7L);
        assertThat(json.get("address").asText()).isEqualTo("서울시 강남구");
    }

    @Test
    @Order(2)
    void 비즈니스_트랜잭션이_롤백되면_이벤트도_남지_않는다() {
        transactionTemplate.executeWithoutResult(status -> {
            outboxEventRecorder.record(UUID.randomUUID(), "HUB", sampleDto());
            status.setRollbackOnly();
        });

        assertThat(outboxEventRepository.count()).isZero();
    }

    @Test
    @Order(3)
    void 트랜잭션_밖에서_기록하면_예외() {
        assertThatThrownBy(() -> outboxEventRecorder.record(UUID.randomUUID(), "HUB", sampleDto()))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    @Order(4)
    void SKIP_LOCKED_다른_인스턴스가_잡은_이벤트는_건너뛴다() throws Exception {
        recordInNewTx(3);
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Integer> instanceA = executor.submit(() -> transactionTemplate.execute(status -> {
                int size = outboxEventRepository.findPublishableForUpdate(java.time.LocalDateTime.now(), 100).size();
                locked.countDown();
                awaitLatch(release);
                return size;
            }));
            assertThat(locked.await(10, TimeUnit.SECONDS)).isTrue();

            // 인스턴스 A 가 잠근 동안 B 는 같은 행을 가져가지 못함 (대기도 하지 않음)
            Integer seenByB = transactionTemplate.execute(status ->
                    outboxEventRepository.findPublishableForUpdate(java.time.LocalDateTime.now(), 100).size());
            assertThat(seenByB).isZero();

            release.countDown();
            assertThat(instanceA.get(10, TimeUnit.SECONDS)).isEqualTo(3);

            Integer afterRelease = transactionTemplate.execute(status ->
                    outboxEventRepository.findPublishableForUpdate(java.time.LocalDateTime.now(), 100).size());
            assertThat(afterRelease).isEqualTo(3);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    @Order(5)
    void 브로커_장애시_유실없이_PENDING_으로_남고_복구되면_발행된다() throws Exception {
        OutboxEvent event = recordInNewTx(1).get(0);

        kafka.getDockerClient().pauseContainerCmd(kafka.getContainerId()).exec();
        long elapsedMs;
        try {
            long start = System.nanoTime();
            int published = outboxPublisher.publishPendingEvents().published();
            elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            assertThat(published).isZero();
            OutboxEvent failed = outboxEventRepository.findById(event.getId()).orElseThrow();
            assertThat(failed.getStatus()).isEqualTo(OutboxStatus.PENDING);
            assertThat(failed.getRetryCount()).isEqualTo(1);
            assertThat(failed.getLastError()).containsIgnoringCase("timeout");
            assertThat(failed.getNextRetryAt()).isAfter(failed.getCreatedAt());

            // 장애가 계속되면 백오프 동안은 재시도하지 않고, 이후에도 FAILED 로 버리지 않음
            outboxPublisher.publishPendingEvents();
            assertThat(outboxEventRepository.findById(event.getId()).orElseThrow().getRetryCount()).isEqualTo(1);
        } finally {
            kafka.getDockerClient().unpauseContainerCmd(kafka.getContainerId()).exec();
        }
        // 브로커 장애 시 기본값(60s) 만큼 블로킹되지 않고 빠르게 실패
        assertThat(elapsedMs).isLessThan(21_000);
        System.out.printf("[IT] 브로커 장애 시 발행 실패까지 걸린 시간: %dms%n", elapsedMs);

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            outboxRelay.relay();
            assertThat(outboxEventRepository.findById(event.getId()).orElseThrow().getStatus())
                    .isEqualTo(OutboxStatus.PUBLISHED);
        });
        // 브로커 멈춤 중 timeout 난 요청이 복구 후 실제로 기록될 수 있음 (결과 불명확한 실패) → 재발행으로 중복 가능.
        // 유실은 없어야 하고, 중복분은 같은 eventId 를 가지므로 컨슈머가 제거한다.
        List<ConsumerRecord<String, String>> records = pollAll(event.getId());
        assertThat(records).isNotEmpty();
        assertThat(records).allSatisfy(r -> assertThat(r.key()).isEqualTo(event.getAggregateId().toString()));
        System.out.printf("[IT] 브로커 복구 후 eventId=%s 수신 건수: %d (1 초과분은 at-least-once 중복 → 컨슈머 eventId 로 제거)%n",
                event.getId(), records.size());
    }

    // ---------------------------------------------------------------------------------------------

    private List<OutboxEvent> recordInNewTx(int count) {
        return transactionTemplate.execute(status -> {
            List<OutboxEvent> events = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                events.add(outboxEventRecorder.record(UUID.randomUUID(), "HUB", sampleDto()));
            }
            return events;
        });
    }

    /** eventId 헤더로 해당 이벤트 레코드를 찾는다 (정확히 1건이어야 함) */
    private ConsumerRecord<String, String> pollOne(UUID eventId) {
        List<ConsumerRecord<String, String>> found = pollAll(eventId);
        assertThat(found).as("eventId=%s 메시지는 정확히 1건 발행되어야 함", eventId).hasSize(1);
        return found.get(0);
    }

    private List<ConsumerRecord<String, String>> pollAll(UUID eventId) {
        List<ConsumerRecord<String, String>> found = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            for (ConsumerRecord<String, String> r : hubConsumer.poll(Duration.ofMillis(500))) {
                var header = r.headers().lastHeader(OutboxPublisher.EVENT_ID_HEADER);
                if (header != null && new String(header.value(), StandardCharsets.UTF_8).equals(eventId.toString())) {
                    found.add(r);
                }
            }
            if (!found.isEmpty()) {
                // 추가 중복이 없는지 잠시 더 확인
                hubConsumer.poll(Duration.ofSeconds(1)).forEach(r -> {
                    var header = r.headers().lastHeader(OutboxPublisher.EVENT_ID_HEADER);
                    if (header != null && new String(header.value(), StandardCharsets.UTF_8).equals(eventId.toString())) {
                        found.add(r);
                    }
                });
                break;
            }
        }
        return found;
    }

    private static <T> T single(List<T> list) {
        assertThat(list).hasSize(1);
        return list.get(0);
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Delivery pendingDelivery() {
        return Delivery.builder()
                .orderId(UUID.randomUUID())
                .departureHubId(UUID.randomUUID())
                .destinationHubId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .recipientCompany(RecipientCompany.builder()
                        .companyId(UUID.randomUUID())
                        .address("서울시 강남구")
                        .slackId("U-COMPANY")
                        .phone("010-0000-0000")
                        .build())
                .build();
    }

    private static DeliveryInfoDto sampleDto() {
        return DeliveryInfoDto.builder()
                .orderId(UUID.randomUUID())
                .departureHubId(UUID.randomUUID())
                .destinationHubId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .slackId("U-TEST")
                .address("부산시")
                .build();
    }
}
