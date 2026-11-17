package com.sparta.logistics.delivery_service.domain.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * 주문 단위 배송 취소 표시.
 * 취소 요청이 배송 생성보다 먼저 끝난 경우(생성 응답이 끊겨 주문이 곧바로 보상한 경우),
 * 늦게 도착한 배송 생성을 거절하기 위해 남긴다.
 */
@Entity
@Getter
@Table(name = "p_delivery_cancellation")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class DeliveryCancellation {

    @Id
    @Column(name = "order_id")
    private UUID orderId;

    @Column(name = "cancelled_at", nullable = false)
    private LocalDateTime cancelledAt;

    public DeliveryCancellation(UUID orderId, LocalDateTime cancelledAt) {
        this.orderId = orderId;
        this.cancelledAt = cancelledAt;
    }
}
