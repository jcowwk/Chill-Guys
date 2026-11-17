package com.sparta.logistics.order_service.infrastructure.client;

import com.sparta.logistics.order_service.infrastructure.client.dto.request.OrderDeliveryRequestDto;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;

import java.util.UUID;

@FeignClient(name = "delivery-service")
public interface DeliveryClient {

    @PostMapping("/api/deliveries")
    UUID createDelivery(@RequestBody OrderDeliveryRequestDto requestDto,
                        @RequestHeader(value = "X-User-Id") String userIdHeader
    );

    /** 주문 생성 실패 보상: 이 주문의 배송 취소 (없으면 0, 여러 번 호출해도 안전) */
    @PostMapping("/api/deliveries/orders/{orderId}/cancel")
    Integer cancelDeliveriesByOrder(@PathVariable("orderId") UUID orderId);
}