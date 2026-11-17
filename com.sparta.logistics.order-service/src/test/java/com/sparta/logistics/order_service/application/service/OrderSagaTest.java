package com.sparta.logistics.order_service.application.service;

import com.sparta.logistics.order_service.application.dto.request.OrderCreateRequestDto;
import com.sparta.logistics.order_service.domain.Order;
import com.sparta.logistics.order_service.domain.OrderStatus;
import com.sparta.logistics.order_service.infrastructure.client.DeliveryClient;
import com.sparta.logistics.order_service.infrastructure.client.ProductClient;
import com.sparta.logistics.order_service.infrastructure.client.dto.request.ProductStockRequestDto;
import com.sparta.logistics.order_service.infrastructure.client.dto.response.StockUpdateResponseDto;
import com.sparta.logistics.order_service.repository.OrderRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 주문 생성 Saga 검증 (실제 PostgreSQL). 재고·배송 서비스는 mock 으로 성공/실패를 주입한다.
 */
@DataJpaTest
@Testcontainers(disabledWithoutDocker = true)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({OrderService.class, OrderSagaStore.class, OrderRecoveryScheduler.class})
@Transactional(propagation = Propagation.NOT_SUPPORTED) // 단계마다 실제로 커밋되는지 확인하기 위해
class OrderSagaTest {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("DDL_AUTO", () -> "create-drop");
    }

    @Autowired OrderService orderService;
    @Autowired OrderRecoveryScheduler recoveryScheduler;
    @Autowired OrderRepository orderRepository;
    @MockitoBean ProductClient productClient;
    @MockitoBean DeliveryClient deliveryClient;

    private final UUID productId = UUID.randomUUID();

    @AfterEach
    void tearDown() {
        orderRepository.deleteAll();
    }

    @Test
    @DisplayName("재고·배송 성공 → 주문 접수(PENDING), 재고 차감 요청에 주문 ID 를 담아 보낸다")
    void success_confirmsOrder_andSendsOrderId() {
        UUID deliveryId = UUID.randomUUID();
        when(productClient.decreaseStock(any(), any())).thenReturn(stock(true));
        when(deliveryClient.createDelivery(any(), any())).thenReturn(deliveryId);

        UUID orderId = orderService.createOrder(request(), "1").getOrderId();

        Order order = orderRepository.findById(orderId).orElseThrow();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.getDeliveryId()).isEqualTo(deliveryId);
        assertThat(order.isStockKeyed()).isTrue();
        ArgumentCaptor<ProductStockRequestDto> captor = ArgumentCaptor.forClass(ProductStockRequestDto.class);
        verify(productClient).decreaseStock(eq(productId), captor.capture());
        assertThat(captor.getValue().getOrderId()).isEqualTo(orderId);
        verify(productClient, never()).increaseStock(any(), any(), any());
    }

    @Test
    @DisplayName("재고 부족 → FAILED, 깎인 게 없으므로 보상하지 않고 배송도 만들지 않는다")
    void outOfStock_failsWithoutCompensation() {
        when(productClient.decreaseStock(any(), any())).thenReturn(stock(false));

        assertThatThrownBy(() -> orderService.createOrder(request(), "1")).hasMessage("재고 부족");

        Order order = onlyOrder();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.FAILED);
        assertThat(order.getFailureReason()).isEqualTo("재고 부족");
        verifyNoInteractions(deliveryClient);
        verify(productClient, never()).increaseStock(any(), any(), any());
    }

    @Test
    @DisplayName("재고 차감 결과를 모름(timeout) → 주문 ID 로 보상 후 FAILED (실제로 안 깎였으면 product 가 무시)")
    void stockTimeout_compensatesWithOrderId() {
        when(productClient.decreaseStock(any(), any())).thenThrow(new RuntimeException("Read timed out"));

        assertThatThrownBy(() -> orderService.createOrder(request(), "1")).isInstanceOf(RuntimeException.class);

        Order order = onlyOrder();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.FAILED);
        verify(productClient).increaseStock(productId, 2L, order.getOrderId());
        verifyNoInteractions(deliveryClient);
    }

    @Test
    @DisplayName("배송 생성 실패 → 주문 ID 로 재고 보상 후 FAILED")
    void deliveryFailure_compensatesStock() {
        when(productClient.decreaseStock(any(), any())).thenReturn(stock(true));
        when(deliveryClient.createDelivery(any(), any())).thenThrow(new RuntimeException("delivery-service 503"));

        assertThatThrownBy(() -> orderService.createOrder(request(), "1"))
                .hasMessage("배송 생성 실패로 주문을 완료할 수 없습니다.");

        Order order = onlyOrder();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.FAILED);
        assertThat(order.getFailureReason()).startsWith("배송 생성 실패");
        // 응답만 끊기고 배송은 만들어졌을 수 있으므로 배송 취소 → 재고 복구 순서로 보상
        var inOrder = inOrder(deliveryClient, productClient);
        inOrder.verify(deliveryClient).cancelDeliveriesByOrder(order.getOrderId());
        inOrder.verify(productClient).increaseStock(productId, 2L, order.getOrderId());
    }

    @Test
    @DisplayName("배송 취소 호출이 실패하면 보상을 끝내지 않고 PROCESSING 으로 남겨 복구 스케줄러가 다시 시도한다")
    void deliveryCancelFailure_staysProcessing_thenRecovered() {
        when(productClient.decreaseStock(any(), any())).thenReturn(stock(true));
        when(deliveryClient.createDelivery(any(), any())).thenThrow(new RuntimeException("Read timed out"));
        when(deliveryClient.cancelDeliveriesByOrder(any())).thenThrow(new RuntimeException("delivery-service down"));

        assertThatThrownBy(() -> orderService.createOrder(request(), "1")).isInstanceOf(RuntimeException.class);
        assertThat(onlyOrder().getStatus()).isEqualTo(OrderStatus.PROCESSING);
        verify(productClient, never()).increaseStock(any(), any(), any());

        reset(deliveryClient);
        when(deliveryClient.cancelDeliveriesByOrder(any())).thenReturn(1);
        when(productClient.increaseStock(any(), any(), any())).thenReturn(stock(true));
        recoveryScheduler.recover(Instant.now().plusSeconds(60));

        assertThat(onlyOrder().getStatus()).isEqualTo(OrderStatus.FAILED);
        verify(deliveryClient).cancelDeliveriesByOrder(onlyOrder().getOrderId());
    }

    @Test
    @DisplayName("알림 서비스용 주문 정보에 주문 상태를 담아 준다")
    void orderInfo_includesStatus() {
        when(productClient.decreaseStock(any(), any())).thenReturn(stock(false));
        assertThatThrownBy(() -> orderService.createOrder(request(), "1")).isInstanceOf(RuntimeException.class);

        assertThat(orderService.getOrderInfo(onlyOrder().getOrderId()).getStatus()).isEqualTo("FAILED");
    }

    @Test
    @DisplayName("보상까지 실패하면 PROCESSING 으로 남고, 복구 스케줄러가 다시 보상해 FAILED 로 정리한다")
    void compensationFailure_recoveredByScheduler() {
        when(productClient.decreaseStock(any(), any())).thenReturn(stock(true));
        when(deliveryClient.createDelivery(any(), any())).thenThrow(new RuntimeException("delivery-service 503"));
        when(productClient.increaseStock(any(), any(), any())).thenThrow(new RuntimeException("product-service down"));

        assertThatThrownBy(() -> orderService.createOrder(request(), "1")).isInstanceOf(RuntimeException.class);
        assertThat(onlyOrder().getStatus()).isEqualTo(OrderStatus.PROCESSING);

        // product 복구 후 스케줄러 실행 (기준 시간을 미래로 줘서 방금 만든 주문도 대상에 포함)
        reset(productClient);
        when(productClient.increaseStock(any(), any(), any())).thenReturn(stock(true));
        int recovered = recoveryScheduler.recover(Instant.now().plusSeconds(60));

        assertThat(recovered).isEqualTo(1);
        assertThat(onlyOrder().getStatus()).isEqualTo(OrderStatus.FAILED);
        verify(productClient).increaseStock(productId, 2L, onlyOrder().getOrderId());
        // 어느 단계에서 멈췄는지 모르므로 복구 때는 배송 취소도 함께
        verify(deliveryClient, atLeastOnce()).cancelDeliveriesByOrder(onlyOrder().getOrderId());
    }

    @Test
    @DisplayName("복구 스케줄러는 기준 시간이 안 지난(아직 진행 중일 수 있는) 주문은 건드리지 않는다")
    void scheduler_skipsRecentProcessingOrders() {
        when(productClient.decreaseStock(any(), any())).thenReturn(stock(true));
        when(deliveryClient.createDelivery(any(), any())).thenThrow(new RuntimeException("503"));
        when(productClient.increaseStock(any(), any(), any())).thenThrow(new RuntimeException("down"));
        assertThatThrownBy(() -> orderService.createOrder(request(), "1")).isInstanceOf(RuntimeException.class);

        int recovered = recoveryScheduler.recover(Instant.now().minusSeconds(300));

        assertThat(recovered).isZero();
        assertThat(onlyOrder().getStatus()).isEqualTo(OrderStatus.PROCESSING);
    }

    @Test
    @DisplayName("주문 삭제: 새 주문은 주문 ID 로, 예전 주문은 주문 ID 없이 복구, FAILED 는 복구 안 함, PROCESSING 은 거절")
    void deleteOrder_restoresStockAccordingToHowItWasDecreased() {
        when(productClient.increaseStock(any(), any(), any())).thenReturn(stock(true));
        Order keyed = save(OrderStatus.PENDING, true);
        Order legacy = save(OrderStatus.PENDING, null);
        Order failed = save(OrderStatus.FAILED, true);
        Order processing = save(OrderStatus.PROCESSING, true);

        orderService.deleteOrder(keyed.getOrderId(), "1");
        orderService.deleteOrder(legacy.getOrderId(), "1");
        orderService.deleteOrder(failed.getOrderId(), "1");
        assertThatThrownBy(() -> orderService.deleteOrder(processing.getOrderId(), "1"))
                .isInstanceOf(IllegalStateException.class);

        verify(productClient).increaseStock(productId, 2L, keyed.getOrderId());
        verify(productClient).increaseStock(productId, 2L, null);
        verify(productClient, times(2)).increaseStock(any(), any(), any());
    }

    private Order onlyOrder() {
        assertThat(orderRepository.count()).isEqualTo(1);
        return orderRepository.findAll().get(0);
    }

    private Order save(OrderStatus status, Boolean stockKeyed) {
        Order order = Order.processing(UUID.randomUUID(), request(), 1L);
        order.setStatus(status);
        order.setStockKeyed(stockKeyed);
        order.setCreatedAt(Timestamp.from(Instant.now()));
        return orderRepository.save(order);
    }

    private OrderCreateRequestDto request() {
        return OrderCreateRequestDto.builder()
                .productId(productId).requestCompanyId(UUID.randomUUID()).responseCompanyId(UUID.randomUUID())
                .slackId("U-1").phone("010-0000-0000").address("서울시").quantity(2L).request("빠른 배송").build();
    }

    private static StockUpdateResponseDto stock(boolean success) {
        StockUpdateResponseDto dto = new StockUpdateResponseDto();
        org.springframework.test.util.ReflectionTestUtils.setField(dto, "success", success);
        return dto;
    }
}
