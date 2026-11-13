package com.sparta.logistics.delivery_service.infrastructure.monitoring;

import com.sparta.logistics.delivery_service.domain.model.DeliveryStatus;
import com.sparta.logistics.delivery_service.domain.model.OutboxStatus;
import com.sparta.logistics.delivery_service.domain.repository.DeliveryRepository;
import com.sparta.logistics.delivery_service.domain.repository.OutboxEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 배송 → Kafka 구간의 운영 지표 (Prometheus: /actuator/prometheus, 관리 포트).
 *
 * 대기/실패 건수는 DB 를 세야 하므로, 수집 요청마다 조회하지 않고 주기적으로 갱신한 값을 노출한다.
 * 지표 이름/태그에 주문 ID, 주소 같은 개인정보를 넣지 않는다.
 */
@Slf4j
@Component
public class DeliveryPipelineMetrics {

    private final OutboxEventRepository outboxEventRepository;
    private final DeliveryRepository deliveryRepository;

    private final AtomicLong outboxPending = new AtomicLong();
    private final AtomicLong outboxFailed = new AtomicLong();
    private final AtomicLong outboxOldestPendingAgeSeconds = new AtomicLong();
    private final AtomicLong assignmentPending = new AtomicLong();

    private final Counter published;
    private final Counter publishRetriableFailures;
    private final Counter publishPermanentFailures;
    private final Counter assigned;
    private final Counter assignFailed;

    public DeliveryPipelineMetrics(MeterRegistry registry,
                                   OutboxEventRepository outboxEventRepository,
                                   DeliveryRepository deliveryRepository) {
        this.outboxEventRepository = outboxEventRepository;
        this.deliveryRepository = deliveryRepository;

        Gauge.builder("delivery.outbox.pending", outboxPending, AtomicLong::get)
                .description("Kafka 로 아직 발행하지 못한 Outbox 이벤트 수").register(registry);
        Gauge.builder("delivery.outbox.failed", outboxFailed, AtomicLong::get)
                .description("영구 실패로 격리된 Outbox 이벤트 수 (수동 확인 필요)").register(registry);
        Gauge.builder("delivery.outbox.oldest.pending.age", outboxOldestPendingAgeSeconds, AtomicLong::get)
                .baseUnit("seconds").description("가장 오래 대기 중인 Outbox 이벤트의 대기 시간").register(registry);
        Gauge.builder("delivery.assignment.pending", assignmentPending, AtomicLong::get)
                .description("담당자 배정을 기다리는 배송 수").register(registry);

        this.published = Counter.builder("delivery.outbox.published").description("Kafka 발행 성공").register(registry);
        this.publishRetriableFailures = Counter.builder("delivery.outbox.publish.failures")
                .tag("type", "retriable").description("Kafka 발행 실패 (재시도 예정)").register(registry);
        this.publishPermanentFailures = Counter.builder("delivery.outbox.publish.failures")
                .tag("type", "permanent").description("Kafka 발행 실패 (영구 오류)").register(registry);
        this.assigned = Counter.builder("delivery.assignment.result").tag("result", "assigned").register(registry);
        this.assignFailed = Counter.builder("delivery.assignment.result").tag("result", "failed").register(registry);
    }

    public void published(int count) {
        published.increment(count);
    }

    public void publishFailed(boolean retriable) {
        (retriable ? publishRetriableFailures : publishPermanentFailures).increment();
    }

    public void assignmentResult(int assignedCount, int failedCount) {
        assigned.increment(assignedCount);
        assignFailed.increment(failedCount);
    }

    @Scheduled(fixedDelayString = "${monitoring.refresh-ms:10000}")
    public void refresh() {
        try {
            outboxPending.set(outboxEventRepository.countByStatus(OutboxStatus.PENDING));
            outboxFailed.set(outboxEventRepository.countByStatus(OutboxStatus.FAILED));
            LocalDateTime oldest = outboxEventRepository.findOldestCreatedAt(OutboxStatus.PENDING);
            outboxOldestPendingAgeSeconds.set(oldest == null ? 0 : Duration.between(oldest, LocalDateTime.now()).toSeconds());
            assignmentPending.set(deliveryRepository.countByDeliveryStatusAndDeletedAtIsNull(DeliveryStatus.PENDING));
        } catch (Exception e) {
            log.warn("[Metrics] 파이프라인 지표 갱신 실패: {}", e.toString());
        }
    }
}
