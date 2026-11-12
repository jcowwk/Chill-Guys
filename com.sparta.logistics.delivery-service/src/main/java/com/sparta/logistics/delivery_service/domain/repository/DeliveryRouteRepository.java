package com.sparta.logistics.delivery_service.domain.repository;

import com.sparta.logistics.delivery_service.domain.model.Delivery;
import com.sparta.logistics.delivery_service.domain.model.DeliveryRoute;
import com.sparta.logistics.delivery_service.domain.model.DeliveryRouteStatus;
import com.sparta.logistics.delivery_service.domain.model.DeliveryStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface DeliveryRouteRepository extends JpaRepository<DeliveryRoute, UUID> {

    List<DeliveryRoute> findByDeliveryIdAndDeletedAtIsNull(UUID deliveryId);

    Optional<DeliveryRoute> findByIdAndDeliveryIdAndDeletedAtIsNull(UUID routesId, UUID deliveryId);

    List<DeliveryRoute> findByStatusAndDeletedAtIsNull(DeliveryRouteStatus deliveryStatus);

    @Query(value = """
            SELECT id AS "id", COALESCE(created_at, TIMESTAMP 'epoch') AS "createdAt" FROM p_delivery_route
            WHERE status = 'PENDING' AND deleted_at IS NULL
              AND (COALESCE(created_at, TIMESTAMP 'epoch'), id) > (:afterCreatedAt, :afterId)
            ORDER BY COALESCE(created_at, TIMESTAMP 'epoch'), id
            LIMIT :limit
            """, nativeQuery = true)
    List<PendingRef> findPendingRefsAfter(@Param("afterCreatedAt") LocalDateTime afterCreatedAt,
                                          @Param("afterId") UUID afterId,
                                          @Param("limit") int limit);

    @Query(value = """
            SELECT * FROM p_delivery_route
            WHERE id = :id AND status = 'PENDING' AND deleted_at IS NULL
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    Optional<DeliveryRoute> findPendingByIdForUpdateSkipLocked(@Param("id") UUID id);
}
