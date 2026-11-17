package com.sparta.logistics.delivery_service.domain.repository;

import com.sparta.logistics.delivery_service.domain.model.Delivery;
import com.sparta.logistics.delivery_service.domain.model.DeliveryStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface DeliveryRepository extends JpaRepository<Delivery, UUID> {


    Optional<Delivery> findByIdAndDeletedAtIsNull(UUID deliveryId);

    List<Delivery> findByAndDeletedAtIsNull();

    List<Delivery> findByDeliveryStatusAndDeletedAtIsNull(DeliveryStatus deliveryStatus);

    long countByDeliveryStatusAndDeletedAtIsNull(DeliveryStatus deliveryStatus);

    List<Delivery> findByOrderIdAndDeletedAtIsNull(UUID orderId);

    /** 배정 대기 건 ID 를 (생성 시각, ID) 커서 이후로 limit 건 조회 */
    @Query(value = """
            SELECT id AS "id", COALESCE(created_at, TIMESTAMP 'epoch') AS "createdAt" FROM p_delivery
            WHERE delivery_status = 'PENDING' AND deleted_at IS NULL
              AND (COALESCE(created_at, TIMESTAMP 'epoch'), id) > (:afterCreatedAt, :afterId)
            ORDER BY COALESCE(created_at, TIMESTAMP 'epoch'), id
            LIMIT :limit
            """, nativeQuery = true)
    List<PendingRef> findPendingRefsAfter(@Param("afterCreatedAt") LocalDateTime afterCreatedAt,
                                          @Param("afterId") UUID afterId,
                                          @Param("limit") int limit);

    /** 아직 대기 중이고 다른 인스턴스가 잡지 않은 배송만 잠금과 함께 조회 (중복 배정 방지) */
    @Query(value = """
            SELECT * FROM p_delivery
            WHERE id = :id AND delivery_status = 'PENDING' AND deleted_at IS NULL
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    Optional<Delivery> findPendingByIdForUpdateSkipLocked(@Param("id") UUID id);
}
