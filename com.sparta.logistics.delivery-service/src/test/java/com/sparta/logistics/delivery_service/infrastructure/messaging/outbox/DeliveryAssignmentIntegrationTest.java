package com.sparta.logistics.delivery_service.infrastructure.messaging.outbox;

import com.sparta.logistics.delivery_service.application.dto.response.DeliveryManagerInfoDto;
import com.sparta.logistics.delivery_service.application.service.DeliveryAssignmentProcessor;
import com.sparta.logistics.delivery_service.application.service.DeliveryService;
import com.sparta.logistics.delivery_service.application.service.PendingAssignmentLoop;
import com.sparta.logistics.delivery_service.domain.model.*;
import com.sparta.logistics.delivery_service.domain.repository.DeliveryRepository;
import com.sparta.logistics.delivery_service.domain.repository.OutboxEventRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * 배송 담당자 배정을 건별 트랜잭션으로 처리하는지 실제 PostgreSQL 로 검증한다.
 * (기존: 대기 건 전체를 한 트랜잭션 → 한 건 실패 시 전체 롤백, 다중 인스턴스 중복 배정)
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class DeliveryAssignmentIntegrationTest extends OutboxIntegrationTestSupport {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registerCommonProperties(registry, postgres);
        registry.add("spring.kafka.bootstrap-servers", () -> "localhost:1"); // Outbox 기록만 확인 (발행 안 함)
        registry.add("delivery.assignment.batch-size", () -> "2");
    }

    @Autowired DeliveryService deliveryService;
    @Autowired DeliveryAssignmentProcessor processor;
    @Autowired DeliveryRepository deliveryRepository;
    @Autowired OutboxEventRepository outboxEventRepository;
    @Autowired TransactionTemplate tx;

    @AfterEach
    void cleanUp() {
        outboxEventRepository.deleteAll();
        deliveryRepository.deleteAll();
    }

    @Test
    void 한_건의_배정이_실패해도_나머지는_커밋되고_실패_건은_다음_실행에서_배정된다() {
        Delivery ok1 = save(), failing = save(), ok2 = save();
        DeliveryManagerInfoDto m7 = manager(7L);
        when(deliveryManagerClient.assignDeliveryManager(any(), any(), eq("HUB"))).thenReturn(m7);
        when(deliveryManagerClient.assignDeliveryManager(eq(failing.getDepartureHubId()), any(), eq("HUB")))
                .thenThrow(new IllegalStateException("delivery-manager-service 500"));

        PendingAssignmentLoop.Result first = deliveryService.assignPendingDeliveries();

        assertThat(first.assigned()).isEqualTo(2);
        assertThat(first.failed()).isEqualTo(1);
        assertThat(status(ok1)).isEqualTo(DeliveryStatus.SHIPPING);
        assertThat(status(ok2)).isEqualTo(DeliveryStatus.SHIPPING);
        assertThat(status(failing)).isEqualTo(DeliveryStatus.PENDING);
        // 실패 건은 Outbox 도 없다 (상태 변경과 이벤트가 함께 롤백)
        assertThat(outboxEventRepository.findAll()).extracting(OutboxEvent::getAggregateId)
                .containsExactlyInAnyOrder(ok1.getId(), ok2.getId());

        reset(deliveryManagerClient);
        DeliveryManagerInfoDto m8 = manager(8L);
        when(deliveryManagerClient.assignDeliveryManager(any(), any(), eq("HUB"))).thenReturn(m8);

        PendingAssignmentLoop.Result second = deliveryService.assignPendingDeliveries();

        assertThat(second.assigned()).isEqualTo(1);
        assertThat(status(failing)).isEqualTo(DeliveryStatus.SHIPPING);
        assertThat(outboxEventRepository.count()).isEqualTo(3);
    }

    @Test
    void 다른_인스턴스가_잡고_있는_배송은_건너뛰고_원격_배정도_호출하지_않는다() throws Exception {
        Delivery delivery = save();
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService otherInstance = Executors.newSingleThreadExecutor();
        try {
            Future<?> f = otherInstance.submit(() -> tx.executeWithoutResult(s -> {
                deliveryRepository.findPendingByIdForUpdateSkipLocked(delivery.getId()).orElseThrow();
                locked.countDown();
                try { release.await(10, TimeUnit.SECONDS); } catch (InterruptedException ignored) { }
            }));
            assertThat(locked.await(10, TimeUnit.SECONDS)).isTrue();

            assertThat(processor.assignDelivery(delivery.getId())).isFalse();
            verify(deliveryManagerClient, never()).assignDeliveryManager(any(), any(), any());

            release.countDown();
            f.get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            otherInstance.shutdownNow();
        }
    }

    @Test
    void 이미_배정된_배송은_다시_배정하지_않는다() {
        Delivery delivery = save();
        DeliveryManagerInfoDto m1 = manager(1L);
        when(deliveryManagerClient.assignDeliveryManager(any(), any(), any())).thenReturn(m1);
        assertThat(processor.assignDelivery(delivery.getId())).isTrue();

        assertThat(processor.assignDelivery(delivery.getId())).isFalse();
        verify(deliveryManagerClient, times(1)).assignDeliveryManager(any(), any(), any());
        assertThat(outboxEventRepository.count()).isEqualTo(1);
    }

    @Test
    void 하위_서비스가_죽어_배치_전체가_실패하면_그_실행을_중단한다() {
        for (int i = 0; i < 6; i++) save();
        when(deliveryManagerClient.assignDeliveryManager(any(), any(), any()))
                .thenThrow(new IllegalStateException("connection refused"));

        PendingAssignmentLoop.Result r = deliveryService.assignPendingDeliveries();

        assertThat(r.stoppedByFailure()).isTrue();
        verify(deliveryManagerClient, times(2)).assignDeliveryManager(any(), any(), any()); // batch-size=2
        assertThat(deliveryRepository.findAll()).allMatch(d -> d.getDeliveryStatus() == DeliveryStatus.PENDING);
    }

    @Test
    void 인스턴스_두_대가_동시에_배정해도_각_배송은_정확히_한_번만_배정된다() throws Exception {
        for (int i = 0; i < 20; i++) save();
        DeliveryManagerInfoDto m = manager(3L);
        when(deliveryManagerClient.assignDeliveryManager(any(), any(), any())).thenAnswer(inv -> {
            Thread.sleep(20); // 원격 호출 지연 → 두 인스턴스의 처리 구간이 겹치도록
            return m;
        });

        ExecutorService instances = Executors.newFixedThreadPool(2);
        try {
            Future<PendingAssignmentLoop.Result> a = instances.submit(() -> deliveryService.assignPendingDeliveries());
            Future<PendingAssignmentLoop.Result> b = instances.submit(() -> deliveryService.assignPendingDeliveries());
            int assigned = a.get(60, TimeUnit.SECONDS).assigned() + b.get(60, TimeUnit.SECONDS).assigned();

            assertThat(assigned).isEqualTo(20);
            verify(deliveryManagerClient, times(20)).assignDeliveryManager(any(), any(), any());
            assertThat(outboxEventRepository.count()).isEqualTo(20);
            assertThat(outboxEventRepository.findAll()).extracting(OutboxEvent::getAggregateId).doesNotHaveDuplicates();
        } finally {
            instances.shutdownNow();
        }
    }

    private DeliveryStatus status(Delivery d) {
        return deliveryRepository.findById(d.getId()).orElseThrow().getDeliveryStatus();
    }

    private Delivery save() {
        return deliveryRepository.save(Delivery.builder()
                .orderId(UUID.randomUUID()).departureHubId(UUID.randomUUID()).destinationHubId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .recipientCompany(RecipientCompany.builder().companyId(UUID.randomUUID()).address("서울시").build())
                .build());
    }

    private static DeliveryManagerInfoDto manager(long id) {
        DeliveryManagerInfoDto m = mock(DeliveryManagerInfoDto.class);
        when(m.getId()).thenReturn(id);
        when(m.getSlackId()).thenReturn("U-" + id);
        return m;
    }
}
