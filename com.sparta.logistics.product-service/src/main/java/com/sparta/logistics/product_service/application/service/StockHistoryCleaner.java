package com.sparta.logistics.product_service.application.service;

import com.sparta.logistics.product_service.domain.repository.StockHistoryRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * 주문별 재고 변동 기록(p_stock_history)을 주기적으로 정리한다.
 *
 * 기록은 같은 주문의 재고 요청이 다시 왔을 때 한 번만 반영하기 위한 것이다.
 * 차감은 주문 생성 중에만, 복구는 주문이 실패로 끝날 때까지(자동 복구 포함) 오므로
 * 보관 기간은 주문이 PROCESSING 으로 머무를 수 있는 기간보다 충분히 길게 둔다.
 */
@Slf4j
@Component
public class StockHistoryCleaner {

    private final StockHistoryRepository historyRepository;
    private final int retentionDays;

    public StockHistoryCleaner(StockHistoryRepository historyRepository,
                               @Value("${stock.history.retention-days:30}") int retentionDays) {
        this.historyRepository = historyRepository;
        this.retentionDays = retentionDays;
    }

    @Scheduled(cron = "${stock.history.purge-cron:0 40 4 * * *}")
    public void scheduledPurge() {
        try {
            purge(LocalDateTime.now());
        } catch (Exception e) {
            log.error("[StockHistory] 정리 실패: {}", e.toString());
        }
    }

    @Transactional
    public int purge(LocalDateTime now) {
        int deleted = historyRepository.deleteCreatedBefore(now.minusDays(retentionDays));
        log.info("[StockHistory] {}일 지난 재고 변동 기록 {}건 정리", retentionDays, deleted);
        return deleted;
    }
}
