package com.sparta.logistics.product_service.application.dto.request;

import lombok.*;

import java.util.UUID;

@Getter
@Builder
public class ProductStockRequestDto {
    private UUID productId;
    private Long quantity;
    /** 멱등 키. 같은 주문의 차감 요청이 여러 번 와도 재고는 한 번만 깎인다 (없으면 기존 동작) */
    private UUID orderId;
}
