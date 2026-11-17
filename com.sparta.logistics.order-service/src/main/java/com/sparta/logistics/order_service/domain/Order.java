package com.sparta.logistics.order_service.domain;

import com.sparta.logistics.order_service.application.dto.request.OrderCreateRequestDto;
import com.sparta.logistics.order_service.infrastructure.client.dto.request.OrderDeliveryRequestDto;
import jakarta.persistence.*;
import lombok.*;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@Entity
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Table(name = "p_order")
public class Order {

    @Id
    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "delivery_id")
    private UUID deliveryId;

    @Column(name = "product_id", nullable = false)
    private UUID productId;

    @Column(name = "request_company_id", nullable = false)
    private UUID requestCompanyId;

    @Column(name = "response_company_id", nullable = false)
    private UUID responseCompanyId;

    @Column(name = "slack_id")
    private String slackId;

    @Column(name = "phone")
    private String phone;

    @Column(name = "address")
    private String address;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private OrderStatus status;

    @Column(name = "quantity", nullable = false)
    private Long quantity;

    @Column(name = "request", nullable = false)
    private String request;

    @Column(name = "created_by", nullable = false)
    private Long createdBy;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Timestamp createdAt;

    @Column(name = "updated_by", nullable = false)
    private Long updatedBy;

    @Column(name = "updated_at", nullable = false)
    private Timestamp updatedAt;

    @Column(name = "deleted_by")
    private Long deletedBy;

    @Column(name = "deleted_at")
    private Timestamp deletedAt;

    /** 재고를 주문 ID 로 차감했는지. true 면 복구도 주문 ID 로 해서 한 번만 반영된다 (이전에 만든 주문은 null) */
    @Column(name = "stock_keyed")
    private Boolean stockKeyed;

    /** 주문 생성 실패 사유 */
    @Column(name = "failure_reason", length = 500)
    private String failureReason;

    /** 주문 생성 Saga 시작: 재고·배송을 처리하기 전에 진행 기록부터 남긴다 */
    public static Order processing(UUID orderId, OrderCreateRequestDto dto, Long userId) {
        Order order = toEntity(orderId, dto, null, userId);
        order.status = OrderStatus.PROCESSING;
        order.stockKeyed = true;
        return order;
    }

    /** 재고·배송 처리 성공 → 주문 접수 완료 */
    public void confirm(UUID deliveryId) {
        if (this.status != OrderStatus.PROCESSING) {
            throw new IllegalStateException("처리 중인 주문만 확정할 수 있습니다. 현재 상태: " + this.status);
        }
        this.deliveryId = deliveryId;
        this.status = OrderStatus.PENDING;
        this.updatedAt = Timestamp.from(Instant.now());
    }

    /** 주문 생성 실패 (재고 보상 완료) */
    public void fail(String reason) {
        this.status = OrderStatus.FAILED;
        this.failureReason = reason == null ? null : reason.substring(0, Math.min(reason.length(), 500));
        this.updatedAt = Timestamp.from(Instant.now());
    }

    public boolean isStockKeyed() {
        return Boolean.TRUE.equals(stockKeyed);
    }

    public static Order toEntity(UUID orderId, OrderCreateRequestDto dto, UUID deliveryId, Long userId) {
        return Order.builder()
                .orderId(orderId)
                .deliveryId(deliveryId)
                .productId(dto.getProductId())
                .requestCompanyId(dto.getRequestCompanyId())
                .responseCompanyId(dto.getResponseCompanyId())
                .slackId(dto.getSlackId())
                .phone(dto.getPhone())
                .address(dto.getAddress())
                .status(OrderStatus.PENDING)
                .quantity(dto.getQuantity())
                .request(dto.getRequest())
                .createdBy(userId)
                .createdAt(Timestamp.from(Instant.now()))
                .updatedBy(userId)
                .updatedAt(Timestamp.from(Instant.now()))
                .deletedBy(null)
                .deletedAt(null)
                .build();
    }

    public void deleteOrder(Long deletedBy) {
        this.deletedAt = Timestamp.from(Instant.now());
        this.deletedBy = deletedBy;
    }

    public void updateOrder(OrderStatus status, Long quantity, String request, Long updatedBy) {
        this.status = status;
        this.quantity = quantity;
        this.request = request;
        this.updatedBy = updatedBy;
        this.updatedAt = Timestamp.from(Instant.now());
    }

    // 주문 상태만 변경하는 메서드
    public void updateStatus(OrderStatus newStatus, Long updatedBy) {
        this.status = newStatus;
        this.updatedBy = updatedBy;
        this.updatedAt = Timestamp.from(Instant.now());
    }
}
