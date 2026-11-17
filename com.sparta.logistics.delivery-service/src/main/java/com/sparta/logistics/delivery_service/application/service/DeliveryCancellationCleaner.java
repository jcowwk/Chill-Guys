package com.sparta.logistics.delivery_service.application.service;

import com.sparta.logistics.delivery_service.domain.repository.DeliveryCancellationRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * 주문 단위 배송 취소 표시(p_delivery_cancellation)를 주기적으로 정리한다.
 *
 * 표시는 취소보다 늦게 도착한 배송 생성 요청을 거절하기 위한 것이다.
 * 배송 생성은 주문 생성 요청 안에서만 오므로(응답 timeout 수 초) 보관 기간은 넉넉히 둔다.
 */
@Slf4j
@Component
public class DeliveryCancellationCleaner {

    private final DeliveryCancellationRepository cancellationRepository;
    private final int retentionDays;

    public DeliveryCancellationCleaner(DeliveryCancellationRepository cancellationRepository,
                                       @Value("${delivery.cancellation.retention-days:30}") int retentionDays) {
        this.cancellationRepository = cancellationRepository;
        this.retentionDays = retentionDays;
    }

    @Scheduled(cron = "${delivery.cancellation.purge-cron:0 50 4 * * *}")
    public void scheduledPurge() {
        try {
            purge(LocalDateTime.now());
        } catch (Exception e) {
            log.error("[DeliveryCancellation] 정리 실패: {}", e.toString());
        }
    }

    @Transactional
    public int purge(LocalDateTime now) {
        int deleted = cancellationRepository.deleteCancelledBefore(now.minusDays(retentionDays));
        log.info("[DeliveryCancellation] {}일 지난 배송 취소 표시 {}건 정리", retentionDays, deleted);
        return deleted;
    }
}
