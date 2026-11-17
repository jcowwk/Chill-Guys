package com.sparta.logistics.order_service.infrastructure.client;

import com.sparta.logistics.order_service.infrastructure.client.dto.request.ProductStockRequestDto;
import com.sparta.logistics.order_service.infrastructure.client.dto.response.StockUpdateResponseDto;
import com.sparta.logistics.order_service.infrastructure.client.dto.response.StockResponseDto;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@FeignClient(name = "product-service")
public interface ProductClient {

    // 상품 재고 감소 (주문 생성 시)
    @PostMapping("/api/products/{productId}/decrease-stock")
    StockUpdateResponseDto decreaseStock(
            @PathVariable UUID productId,
            @RequestBody ProductStockRequestDto requestDto
    );

    // 상품 재고 복구 (주문 취소 시)
    @PostMapping("/api/products/{productId}/increase-stock")
    StockUpdateResponseDto increaseStock(
            @PathVariable UUID productId,
            @RequestParam Long quantity,
            @RequestParam(required = false) UUID orderId   // null 이면 보내지 않음 (기존 방식)
    );
}


