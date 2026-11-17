package com.sparta.logistics.product_service.domain.entity;

public enum StockChangeType {
    DECREASE,   // 주문으로 인한 차감
    RESTORE     // 주문 취소/실패로 인한 복구 (보상)
}
