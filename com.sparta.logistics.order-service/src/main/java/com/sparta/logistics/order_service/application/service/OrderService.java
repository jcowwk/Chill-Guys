package com.sparta.logistics.order_service.application.service;

import com.sparta.logistics.order_service.infrastructure.client.DeliveryClient;
import com.sparta.logistics.order_service.infrastructure.client.ProductClient;
import com.sparta.logistics.order_service.domain.Order;
import com.sparta.logistics.order_service.domain.OrderStatus;
import com.sparta.logistics.order_service.application.dto.request.OrderCreateRequestDto;
import com.sparta.logistics.order_service.infrastructure.client.dto.request.OrderDeliveryRequestDto;
import com.sparta.logistics.order_service.application.dto.request.OrderUpdateRequestDto;
import com.sparta.logistics.order_service.infrastructure.client.dto.request.ProductStockRequestDto;
import com.sparta.logistics.order_service.application.dto.response.OrderDetailResponseDto;
import com.sparta.logistics.order_service.application.dto.response.PageResponseDto;
import com.sparta.logistics.order_service.infrastructure.client.dto.response.OrderSlackDto;
import com.sparta.logistics.order_service.infrastructure.client.dto.response.StockUpdateResponseDto;
import com.sparta.logistics.order_service.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final ProductClient productClient;
    private final DeliveryClient deliveryClient;
    private final OrderSagaStore orderSagaStore;

    /**
     * 주문 생성 Saga (오케스트레이션).
     *
     * 재고(product-db), 배송(delivery-db), 주문(order-db)은 DB 가 달라 한 트랜잭션으로 묶을 수 없다.
     * 그래서 진행 기록을 먼저 남기고, 단계마다 실패하면 앞 단계를 되돌린다(보상).
     *
     *   1. 주문을 PROCESSING 으로 저장 (진행 기록)
     *   2. 재고 차감 (주문 ID 로 멱등)        실패 → 재고 부족이면 FAILED, 결과 불명이면 보상 후 FAILED
     *   3. 배송 생성                         실패 → 재고 보상 후 FAILED
     *   4. 주문 확정 (PENDING = 접수 완료)
     *
     * 보상까지 실패하면 PROCESSING 으로 남고, OrderRecoveryScheduler 가 다시 보상한다.
     */
    public OrderDetailResponseDto createOrder(OrderCreateRequestDto requestDto, String userIdHeader) {
        Long userId = Long.parseLong(userIdHeader);
        UUID orderId = UUID.randomUUID();
        UUID productId = requestDto.getProductId();
        Long quantity = requestDto.getQuantity();

        // 1. 진행 기록
        orderSagaStore.startProcessing(orderId, requestDto, userId);

        // 2. 재고 차감
        StockUpdateResponseDto stock;
        try {
            stock = productClient.decreaseStock(productId, new ProductStockRequestDto(productId, quantity, orderId));
        } catch (Exception e) {
            // timeout 등: 실제로 깎였는지 알 수 없으므로 되돌린다 (product 가 멱등이라 안 깎였으면 아무 일도 없음)
            compensate(orderId, productId, quantity, "재고 차감 실패: " + e.getClass().getSimpleName(), false);
            throw new RuntimeException("재고 처리 중 오류로 주문을 완료할 수 없습니다.");
        }
        if (stock == null || !stock.isSuccess()) {
            orderSagaStore.fail(orderId, "재고 부족");
            throw new RuntimeException("재고 부족");
        }

        // 3. 배송 생성
        UUID deliveryId;
        try {
            deliveryId = deliveryClient.createDelivery(OrderDeliveryRequestDto.fromOrderRequest(requestDto, orderId), userIdHeader);
        } catch (Exception e) {
            // 응답만 끊기고 배송은 만들어졌을 수 있으므로 배송도 취소한다
            compensate(orderId, productId, quantity, "배송 생성 실패: " + e.getClass().getSimpleName(), true);
            throw new RuntimeException("배송 생성 실패로 주문을 완료할 수 없습니다.");
        }

        // 4. 확정
        Order order = orderSagaStore.confirm(orderId, deliveryId);
        return OrderDetailResponseDto.fromEntity(order);
    }

    /**
     * 보상: (필요하면 배송을 취소하고) 재고를 되돌린 뒤 주문을 FAILED 로.
     * 복구 호출이 실패하면 PROCESSING 으로 남겨 두고 복구 스케줄러가 다시 시도한다.
     * (product 의 복구는 주문 ID 로 멱등하므로 여러 번 시도해도 한 번만 반영된다)
     *
     * @return 보상 완료 여부
     */
    public boolean compensate(UUID orderId, UUID productId, Long quantity, String reason, boolean cancelDelivery) {
        try {
            if (cancelDelivery) {
                deliveryClient.cancelDeliveriesByOrder(orderId);   // 없으면 아무 일도 없음
            }
            productClient.increaseStock(productId, quantity, orderId);
            orderSagaStore.fail(orderId, reason);
            return true;
        } catch (Exception e) {
            log.error("보상 실패 - 복구 스케줄러가 재시도 orderId={}: {}", orderId, e.toString());
            return false;
        }
    }

    // [조회]
    @Transactional(readOnly = true)
    public PageResponseDto<OrderDetailResponseDto> getAllOrders(Pageable pageable) {
        Page<Order> orders = orderRepository.findAllByDeletedAtIsNull(pageable);
        return new PageResponseDto<>(orders.map(OrderDetailResponseDto::fromEntity));
    }

    // [개별 조회]
    @Transactional(readOnly = true)
    public OrderDetailResponseDto getOrderById(UUID orderId) {
        return orderRepository.findByOrderIdAndDeletedAtIsNull(orderId)
                .map(OrderDetailResponseDto::fromEntity)
                .orElseThrow(() -> new RuntimeException("Order not found"));
    }

    // [주문 수정]
    @Transactional
    public OrderDetailResponseDto updateOrder(UUID id, OrderUpdateRequestDto updateDto, String userIdHeader) {
        Order order = orderRepository.findByOrderIdAndDeletedAtIsNull(id)
                .orElseThrow(() -> new RuntimeException("Order not found or already deleted"));

        Long userId = Long.parseLong(userIdHeader); //변환

        order.updateOrder(updateDto.getStatus(), updateDto.getQuantity(), updateDto.getRequest(), userId);

        return OrderDetailResponseDto.fromEntity(order);
    }

    // [삭제]
    @Transactional
    public void deleteOrder(UUID id, String userIdHeader) {
        Long userId = Long.parseLong(userIdHeader); //변환
        Order order = orderRepository.findByOrderIdAndDeletedAtIsNull(id)
                .orElseThrow(() -> new RuntimeException("Order not found or already deleted"));

        if (order.getStatus() == OrderStatus.PROCESSING) {
            throw new IllegalStateException("처리 중인 주문은 삭제할 수 없습니다.");
        }
        // 생성에 실패한 주문은 이미 재고를 되돌렸으므로 다시 복구하지 않는다
        if (order.getStatus() != OrderStatus.FAILED) {
            // 주문 ID 로 차감한 주문은 주문 ID 로 복구 (여러 번 호출돼도 한 번만 반영)
            // 이전에 만든 주문은 product 에 차감 기록이 없으므로 기존 방식으로 복구
            UUID stockKey = order.isStockKeyed() ? order.getOrderId() : null;
            StockUpdateResponseDto response = productClient.increaseStock(order.getProductId(), order.getQuantity(), stockKey);
            if (!response.isSuccess()) {
                throw new RuntimeException("Stock restore failed");
            }
        }

        // 주문 삭제 처리
        order.deleteOrder(userId);
    }

    // [상태만 수정]
    @Transactional
    public void updateOrderStatus(UUID id, OrderStatus newStatus, String userIdHeader) {
        Order order = orderRepository.findByOrderIdAndDeletedAtIsNull(id)
                .orElseThrow(() -> new RuntimeException("Order not found or already deleted"));

        Long userId = Long.parseLong(userIdHeader);

        order.updateStatus(newStatus, userId);
    }


    public OrderSlackDto getOrderInfo(UUID id) {
        Order order  = orderRepository.findByOrderIdAndDeletedAtIsNull(id)
                .orElseThrow(() -> new RuntimeException("Order not found or already deleted"));

        Long quantity = order.getQuantity();
        String request = order.getRequest();

        return OrderSlackDto.builder()
                .quantity(quantity)
                .request(request)
                .status(order.getStatus().name())
                .build();
    }
}
