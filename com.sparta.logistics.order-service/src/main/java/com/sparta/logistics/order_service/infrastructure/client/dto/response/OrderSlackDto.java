package com.sparta.logistics.order_service.infrastructure.client.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class OrderSlackDto {
    private Long quantity;
    private String request;
    /** 주문 상태. 알림 서비스가 실패·취소된 주문의 알림을 보내지 않기 위해 사용 */
    private String status;
}
