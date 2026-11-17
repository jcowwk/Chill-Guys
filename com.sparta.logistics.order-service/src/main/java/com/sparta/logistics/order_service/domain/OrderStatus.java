package com.sparta.logistics.order_service.domain;

public enum OrderStatus {
    PROCESSING, // 재고 차감·배송 생성 진행 중 (주문 생성 Saga 진행 기록)
    PENDING,    // 주문 접수 완료 (재고·배송 처리 성공)
    CONFIRMED,
    DELIVERED,
    CANCELLED,
    FAILED;     // 주문 생성 실패 (재고 보상 완료)
}