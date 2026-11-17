package com.sparta.logistics.product_service.domain.entity;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * 주문별 재고 변동 기록. 같은 주문의 같은 요청이 여러 번 와도 재고를 한 번만 바꾸기 위한 멱등 키.
 *
 * (주문, 상품, 유형)이 유일하다. 결과(applied)도 저장해서, 다시 온 요청에 처음과 같은 답을 돌려준다.
 */
@Getter
@Entity
@Table(name = "p_stock_history",
        uniqueConstraints = @UniqueConstraint(name = "uk_stock_history_order_product_type",
                columnNames = {"order_id", "product_id", "type"}))
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class StockHistory {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "product_id", nullable = false)
    private UUID productId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private StockChangeType type;

    @Column(nullable = false)
    private Long quantity;

    /** 실제로 재고를 바꿨는지 (재고 부족, 차감 기록 없는 복구 등은 false) */
    @Column(nullable = false)
    private boolean applied;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    public StockHistory(UUID orderId, UUID productId, StockChangeType type, Long quantity, boolean applied) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.productId = productId;
        this.type = type;
        this.quantity = quantity;
        this.applied = applied;
        this.createdAt = LocalDateTime.now();
    }
}
