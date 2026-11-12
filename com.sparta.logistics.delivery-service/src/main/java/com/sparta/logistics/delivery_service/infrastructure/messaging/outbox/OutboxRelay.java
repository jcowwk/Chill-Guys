package com.sparta.logistics.delivery_service.infrastructure.messaging.outbox;

import com.sparta.logistics.delivery_service.domain.model.OutboxStatus;
import com.sparta.logistics.delivery_service.domain.repository.OutboxEventRepository;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * Outbox 이벤트를 주기적으로 Kafka 에 발행하는 릴레이.
 * Kafka 가 꺼져 있는 동안은 이벤트가 DB 에 PENDING 으로 남아 있다가 복구되면 순서대로 발행된다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private final OutboxPublisher outboxPublisher;
    private final OutboxEventRepository outboxEventRepository;

    @Value("${outbox.relay.retention-days:7}")
    private int retentionDays;

    private volatile boolean shutdown = false;

    @Scheduled(fixedDelayString = "${outbox.relay.fixed-delay-ms:3000}")
    public void relay() {
        if (shutdown) return;
        try {
            int published = outboxPublisher.publishPendingEvents();
            if (published > 0) {
                log.debug("[Outbox] {}건 발행", published);
            }
        } catch (Exception e) {
            // DB 장애 등 - 다음 주기에 다시 시도
            log.error("[Outbox] 릴레이 실행 실패: {}", e.toString());
        }
    }

    /** 장애 감지용: 적체/영구 실패 이벤트가 있으면 경고 로그 */
    @Scheduled(fixedDelayString = "${outbox.relay.monitor-delay-ms:60000}")
    public void monitor() {
        if (shutdown) return;
        try {
            long pending = outboxEventRepository.countByStatus(OutboxStatus.PENDING);
            long failed = outboxEventRepository.countByStatus(OutboxStatus.FAILED);
            if (failed > 0) {
                log.error("[Outbox] FAILED 이벤트 {}건 존재 - 수동 재처리 필요", failed);
            }
            if (pending > 0) {
                log.warn("[Outbox] 미발행 PENDING 이벤트 {}건 (Kafka 장애 여부 확인)", pending);
            }
        } catch (Exception e) {
            log.error("[Outbox] 모니터링 실패: {}", e.toString());
        }
    }

    @Scheduled(cron = "${outbox.relay.purge-cron:0 0 4 * * *}")
    public void purgePublished() {
        if (shutdown) return;
        try {
            int deleted = outboxPublisher.purgePublishedBefore(LocalDateTime.now().minusDays(retentionDays));
            log.info("[Outbox] 발행 완료 이벤트 {}건 정리", deleted);
        } catch (Exception e) {
            log.error("[Outbox] 정리 실패: {}", e.toString());
        }
    }

    @PreDestroy
    public void shutdown() {
        shutdown = true;
    }
}
