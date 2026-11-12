package com.sparta.logistics.delivery_service.domain.repository;

import com.sparta.logistics.delivery_service.domain.model.OutboxEvent;
import com.sparta.logistics.delivery_service.domain.model.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * 발행 대상 이벤트를 행 잠금과 함께 조회한다.
     * SKIP LOCKED: 서비스 인스턴스가 여러 대여도 같은 이벤트를 중복 발행하지 않도록 다른 인스턴스가 잡은 행은 건너뛴다.
     */
    @Query(value = """
            SELECT * FROM p_outbox_event
            WHERE status = 'PENDING' AND next_retry_at <= :now
            ORDER BY created_at
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEvent> findPublishableForUpdate(@Param("now") LocalDateTime now, @Param("limit") int limit);

    long countByStatus(OutboxStatus status);

    @Modifying
    @Query("DELETE FROM OutboxEvent e WHERE e.status = :status AND e.publishedAt < :before")
    int deleteByStatusAndPublishedAtBefore(@Param("status") OutboxStatus status, @Param("before") LocalDateTime before);
}
