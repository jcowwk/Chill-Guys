package com.sparta.logistics.slack_service.infrastructure.client;

import java.util.UUID;

/**
 * 주문이 아직 처리 중(PROCESSING)이라 알림을 보낼지 정할 수 없다.
 * 곧 접수 완료(PENDING) 또는 실패(FAILED)로 확정되므로 일시적 실패로 보고 나중에 다시 처리한다.
 */
public class OrderNotReadyException extends RuntimeException {
    public OrderNotReadyException(UUID orderId) {
        super("주문 처리 중이라 알림 보류 orderId=" + orderId);
    }
}
