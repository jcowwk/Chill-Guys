package com.sparta.logistics.order_service.application.service;

import com.sparta.logistics.order_service.domain.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * 주문 생성 도중 서버가 죽거나 보상까지 실패해 PROCESSING 으로 남은 주문을 정리한다.
 *
 * 그 주문의 배송을 취소하고 재고를 되돌린 뒤 FAILED 로 바꾼다. product 의 복구는 주문 ID 로 멱등이고,
 * 차감 기록이 없으면 "취소 표시"를 남겨 늦게 도착한 차감도 막는다. 그래서 진행 정도를 몰라도 안전하게 되돌릴 수 있다.
 *
 * 기준 시간은 정상 처리의 최대 소요 시간(재고·배송 호출 timeout 합)보다 길어야 한다.
 * 짧으면 아직 진행 중인 주문을 되돌려 버린다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderRecoveryScheduler {

    private final OrderSagaStore orderSagaStore;
    private final OrderService orderService;

    @Value("${order.saga.recover-after-ms:300000}")
    private long recoverAfterMs;

    @Scheduled(fixedDelayString = "${order.saga.recovery-interval-ms:60000}")
    public void recoverStuckOrders() {
        try {
            int recovered = recover(Instant.now().minus(Duration.ofMillis(recoverAfterMs)));
            if (recovered > 0) {
                log.warn("[OrderSaga] 처리 중 멈춘 주문 {}건 보상 후 FAILED 처리", recovered);
            }
        } catch (Exception e) {
            log.error("[OrderSaga] 복구 실행 실패: {}", e.toString());
        }
    }

    /** @return 보상 완료 건수 */
    public int recover(Instant before) {
        List<Order> stuck = orderSagaStore.findStuck(Timestamp.from(before));
        int recovered = 0;
        for (Order order : stuck) {
            if (orderService.compensate(order.getOrderId(), order.getProductId(), order.getQuantity(),
                    "처리 중 중단 - 자동 복구", true)) {   // 어느 단계에서 멈췄는지 모르므로 배송도 취소
                recovered++;
            }
        }
        return recovered;
    }
}
