package com.sparta.logistics.delivery_service.infrastructure.messaging.outbox;

import com.sparta.logistics.delivery_service.application.dto.response.DeliveryManagerInfoDto;
import com.sparta.logistics.delivery_service.application.service.DeliveryService;
import com.sparta.logistics.delivery_service.domain.model.*;
import com.sparta.logistics.delivery_service.domain.repository.DeliveryRepository;
import com.sparta.logistics.delivery_service.domain.repository.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Kafka 가 처음부터 꺼져 있는 상황: 서비스는 정상 기동/동작하고 이벤트는 DB 에 보존되어야 한다.
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class OutboxKafkaUnavailableIntegrationTest extends OutboxIntegrationTestSupport {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registerCommonProperties(registry, postgres);
        registry.add("spring.kafka.bootstrap-servers", () -> "localhost:" + unusedPort());
    }

    @Autowired DeliveryService deliveryService;
    @Autowired DeliveryRepository deliveryRepository;
    @Autowired OutboxEventRepository outboxEventRepository;
    @Autowired OutboxPublisher outboxPublisher;

    @Test
    void Kafka_가_없어도_배송_배정은_성공하고_이벤트는_PENDING_으로_보존된다() {
        Delivery delivery = deliveryRepository.save(Delivery.builder()
                .orderId(UUID.randomUUID())
                .departureHubId(UUID.randomUUID())
                .destinationHubId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .recipientCompany(RecipientCompany.builder()
                        .companyId(UUID.randomUUID()).address("대전시").build())
                .build());
        DeliveryManagerInfoDto manager = mock(DeliveryManagerInfoDto.class);
        when(manager.getId()).thenReturn(1L);
        when(manager.getSlackId()).thenReturn("U-1");
        when(deliveryManagerClient.assignDeliveryManager(any(), any(), any())).thenReturn(manager);

        // 비즈니스 로직은 Kafka 장애와 무관하게 즉시 성공
        long start = System.nanoTime();
        deliveryService.assignPendingDeliveries();
        long assignMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertThat(deliveryRepository.findById(delivery.getId()).orElseThrow().getDeliveryStatus())
                .isEqualTo(DeliveryStatus.SHIPPING);
        assertThat(assignMs).isLessThan(3_000);

        // 릴레이는 빠르게 실패하고 이벤트를 버리지 않음
        start = System.nanoTime();
        int published = outboxPublisher.publishPendingEvents().published();
        long relayMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertThat(published).isZero();
        OutboxEvent event = outboxEventRepository.findAll().get(0);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getRetryCount()).isEqualTo(1);
        assertThat(relayMs).isLessThan(10_000);
        System.out.printf("[IT] Kafka 미기동: 배정 %dms, 발행 실패 감지 %dms, lastError=%s%n",
                assignMs, relayMs, event.getLastError());
    }

    private static int unusedPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
