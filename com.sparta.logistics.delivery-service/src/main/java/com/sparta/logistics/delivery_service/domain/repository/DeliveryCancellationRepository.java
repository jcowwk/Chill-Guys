package com.sparta.logistics.delivery_service.domain.repository;

import com.sparta.logistics.delivery_service.domain.model.DeliveryCancellation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.UUID;

public interface DeliveryCancellationRepository extends JpaRepository<DeliveryCancellation, UUID> {

    /**
     * 같은 주문의 배송 생성과 취소가 한 번에 하나씩만 진행되도록 주문 ID 단위로 잠근다.
     * 트랜잭션이 끝나면 자동으로 풀린다 (PostgreSQL advisory lock).
     */
    @Query(value = "SELECT 1 FROM pg_advisory_xact_lock(hashtextextended(CAST(:orderId AS text), 0))", nativeQuery = true)
    Integer lockOrder(@Param("orderId") UUID orderId);

    @Modifying
    @Query("DELETE FROM DeliveryCancellation c WHERE c.cancelledAt < :before")
    int deleteCancelledBefore(@Param("before") LocalDateTime before);
}
