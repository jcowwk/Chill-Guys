package com.sparta.logistics.product_service.domain.repository;

import com.sparta.logistics.product_service.domain.entity.StockChangeType;
import com.sparta.logistics.product_service.domain.entity.StockHistory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

public interface StockHistoryRepository extends JpaRepository<StockHistory, UUID> {

    Optional<StockHistory> findByOrderIdAndProductIdAndType(UUID orderId, UUID productId, StockChangeType type);

    @Modifying
    @Query("DELETE FROM StockHistory h WHERE h.createdAt < :before")
    int deleteCreatedBefore(@Param("before") LocalDateTime before);
}
