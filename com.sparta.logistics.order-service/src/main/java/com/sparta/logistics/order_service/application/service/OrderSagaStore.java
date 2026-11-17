package com.sparta.logistics.order_service.application.service;

import com.sparta.logistics.order_service.application.dto.request.OrderCreateRequestDto;
import com.sparta.logistics.order_service.domain.Order;
import com.sparta.logistics.order_service.domain.OrderStatus;
import com.sparta.logistics.order_service.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

/**
 * 주문 생성 Saga 의 상태 기록. 단계마다 바로 커밋해서, 중간에 서버가 죽어도 어디까지 진행됐는지 남게 한다.
 *
 * OrderService 와 분리한 이유: 외부 호출(재고, 배송)을 DB 트랜잭션 안에서 하지 않기 위해서다.
 * 원격 호출을 트랜잭션 안에 두면 DB 커넥션을 오래 잡고, 원격에는 반영됐는데 로컬은 롤백되는 일이 생긴다.
 */
@Component
@RequiredArgsConstructor
public class OrderSagaStore {

    private final OrderRepository orderRepository;

    @Transactional
    public Order startProcessing(UUID orderId, OrderCreateRequestDto dto, Long userId) {
        return orderRepository.save(Order.processing(orderId, dto, userId));
    }

    @Transactional
    public Order confirm(UUID orderId, UUID deliveryId) {
        Order order = get(orderId);
        order.confirm(deliveryId);
        return order;
    }

    @Transactional
    public void fail(UUID orderId, String reason) {
        get(orderId).fail(reason);
    }

    @Transactional(readOnly = true)
    public List<Order> findStuck(Timestamp before) {
        return orderRepository.findTop100ByStatusAndCreatedAtBeforeOrderByCreatedAt(OrderStatus.PROCESSING, before);
    }

    private Order get(UUID orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalStateException("주문 없음: " + orderId));
    }
}
