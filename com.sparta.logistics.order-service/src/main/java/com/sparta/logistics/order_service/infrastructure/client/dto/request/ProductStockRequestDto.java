package com.sparta.logistics.order_service.infrastructure.client.dto.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.UUID;


@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProductStockRequestDto {
    private UUID productId;
    private Long quantity;
    /** 멱등 키: 같은 주문의 차감이 여러 번 가도 product-service 는 한 번만 반영한다 */
    private UUID orderId;
}

