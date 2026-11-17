package com.sparta.logistics.product_service.domain.repository;

import com.sparta.logistics.product_service.domain.entity.StockChangeType;
import com.sparta.logistics.product_service.domain.entity.StockHistory;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface StockHistoryRepository extends JpaRepository<StockHistory, UUID> {

    Optional<StockHistory> findByOrderIdAndProductIdAndType(UUID orderId, UUID productId, StockChangeType type);
}
