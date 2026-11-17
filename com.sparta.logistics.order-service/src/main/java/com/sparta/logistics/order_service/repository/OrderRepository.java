package com.sparta.logistics.order_service.repository;

import com.sparta.logistics.order_service.domain.Order;
import com.sparta.logistics.order_service.domain.OrderStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface OrderRepository extends JpaRepository<Order, UUID> {
    Page<Order> findAllByDeletedAtIsNull(Pageable pageable);
    Optional<Order> findByOrderIdAndDeletedAtIsNull(UUID orderId);

    /** 오래 PROCESSING 으로 남은 주문 (처리 중 서버가 죽은 경우) */
    List<Order> findTop100ByStatusAndCreatedAtBeforeOrderByCreatedAt(OrderStatus status, Timestamp before);
}
