package com.sparta.logistics.slack_service.infrastructure.dto;

import lombok.*;

@Data
@Builder
@AllArgsConstructor
public class OrderSlackDto {
    private Long quantity;
    private String request;
    /** 주문 상태 (FAILED, CANCELLED 면 알림을 보내지 않는다) */
    private String status;
}
