package com.sparta.logistics.delivery_service.domain.repository;

import java.time.LocalDateTime;
import java.util.UUID;

/** 배정 대기 건의 커서용 최소 정보 (엔티티 전체를 메모리에 올리지 않기 위함) */
public interface PendingRef {
    UUID getId();

    LocalDateTime getCreatedAt();
}
