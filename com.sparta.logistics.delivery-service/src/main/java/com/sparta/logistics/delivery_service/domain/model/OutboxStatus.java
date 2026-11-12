package com.sparta.logistics.delivery_service.domain.model;

public enum OutboxStatus {
    PENDING,    // 발행 대기 (재시도 대기 포함)
    PUBLISHED,  // Kafka 브로커 ack 완료
    FAILED      // 재시도해도 성공할 수 없는 오류 - 수동 확인 필요
}
