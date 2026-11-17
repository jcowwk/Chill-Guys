package com.sparta.logistics.product_service.application.service;

import com.sparta.logistics.product_service.domain.entity.Product;
import com.sparta.logistics.product_service.domain.entity.StockChangeType;
import com.sparta.logistics.product_service.domain.repository.ProductRepository;
import com.sparta.logistics.product_service.domain.repository.StockHistoryRepository;
import com.sparta.logistics.product_service.infrastructure.client.CompanyClient;
import com.sparta.logistics.product_service.infrastructure.client.HubClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 주문 단위 재고 차감/복구 멱등성 검증 (실제 PostgreSQL, 스레드마다 별도 트랜잭션).
 */
@DataJpaTest
@Testcontainers
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({ProductService.class, StockService.class})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class StockIdempotencyTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired StockService stockService;
    @Autowired ProductService productService;
    @Autowired ProductRepository productRepository;
    @Autowired StockHistoryRepository historyRepository;
    @MockitoBean CompanyClient companyClient;
    @MockitoBean HubClient hubClient;

    @AfterEach
    void tearDown() {
        historyRepository.deleteAll();
        productRepository.deleteAll();
    }

    @Test
    @DisplayName("같은 주문의 차감이 두 번 와도 한 번만 깎이고, 두 번 모두 같은 결과를 돌려준다")
    void duplicateDecrease_appliedOnce() {
        UUID productId = saveProduct(10L);
        UUID orderId = UUID.randomUUID();

        assertThat(stockService.decrease(productId, 3L, orderId).getSuccess()).isTrue();
        assertThat(stockService.decrease(productId, 3L, orderId).getSuccess()).isTrue();

        assertThat(stock(productId)).isEqualTo(7L);
    }

    @Test
    @DisplayName("같은 주문의 차감 20건이 동시에 와도 한 번만 깎인다")
    void concurrentDuplicateDecrease_appliedOnce() throws Exception {
        UUID productId = saveProduct(100L);
        UUID orderId = UUID.randomUUID();

        List<Boolean> results = runConcurrently(20, () -> stockService.decrease(productId, 5L, orderId).getSuccess());

        assertThat(stock(productId)).isEqualTo(95L);
        assertThat(results).hasSize(20).containsOnly(true);
        assertThat(historyRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("재고 부족으로 실패한 차감은 나중에 재고가 채워져도 같은 주문 재요청에 같은 답(실패)을 준다")
    void failedDecrease_sameAnswerOnRetry() {
        UUID productId = saveProduct(1L);
        UUID orderId = UUID.randomUUID();

        assertThat(stockService.decrease(productId, 5L, orderId).getSuccess()).isFalse();
        productService.addStock(productId, 100L); // 재고 보충 (주문과 무관한 입고)

        assertThat(stockService.decrease(productId, 5L, orderId).getSuccess()).isFalse();
        assertThat(stock(productId)).isEqualTo(101L);
    }

    @Test
    @DisplayName("복구가 두 번 와도 한 번만 늘리고, 요청 수량이 아니라 실제 차감 수량으로 되돌린다")
    void duplicateRestore_appliedOnce_withDecreasedQuantity() {
        UUID productId = saveProduct(10L);
        UUID orderId = UUID.randomUUID();
        stockService.decrease(productId, 4L, orderId);

        assertThat(stockService.restore(productId, 999L, orderId).getSuccess()).isTrue();
        assertThat(stockService.restore(productId, 999L, orderId).getSuccess()).isTrue();

        assertThat(stock(productId)).isEqualTo(10L);
    }

    @Test
    @DisplayName("차감 없이 온 복구는 재고를 바꾸지 않고, 이후 늦게 도착한 같은 주문의 차감은 거절한다")
    void restoreBeforeDecrease_blocksLateDecrease() {
        UUID productId = saveProduct(10L);
        UUID orderId = UUID.randomUUID();

        assertThat(stockService.restore(productId, 4L, orderId).getSuccess()).isTrue();
        assertThat(stock(productId)).isEqualTo(10L);

        assertThat(stockService.decrease(productId, 4L, orderId).getSuccess()).isFalse();
        assertThat(stock(productId)).isEqualTo(10L);
        assertThat(historyRepository.findByOrderIdAndProductIdAndType(orderId, productId, StockChangeType.DECREASE))
                .get().extracting(h -> h.isApplied()).isEqualTo(false);
    }

    @Test
    @DisplayName("여러 주문의 차감과 각 주문의 중복 복구가 뒤섞여 동시에 와도 최종 재고가 원래대로 돌아온다")
    void mixedConcurrentDecreaseAndRestore_consistent() throws Exception {
        UUID productId = saveProduct(1_000L);
        List<UUID> orders = new ArrayList<>();
        for (int i = 0; i < 50; i++) orders.add(UUID.randomUUID());

        // 주문마다: 차감 2번, 복구 2번을 동시에 (순서 보장 없음)
        List<Callable<Boolean>> tasks = new ArrayList<>();
        for (UUID orderId : orders) {
            tasks.add(() -> stockService.decrease(productId, 3L, orderId).getSuccess());
            tasks.add(() -> stockService.decrease(productId, 3L, orderId).getSuccess());
            tasks.add(() -> stockService.restore(productId, 3L, orderId).getSuccess());
            tasks.add(() -> stockService.restore(productId, 3L, orderId).getSuccess());
        }
        runAll(tasks);

        // 복구가 먼저 처리된 주문은 차감이 거절되고, 차감이 먼저 처리된 주문은 복구로 되돌아가므로 모두 원상복구
        assertThat(stock(productId)).isEqualTo(1_000L);
        assertThat(historyRepository.count()).isEqualTo(100); // 주문당 차감 1 + 복구 1
    }

    @Test
    @DisplayName("주문 ID 가 없는 기존 호출은 이전처럼 매번 반영된다 (하위 호환)")
    void withoutOrderId_legacyBehavior() {
        UUID productId = saveProduct(10L);

        stockService.decrease(productId, 2L, null);
        stockService.decrease(productId, 2L, null);
        stockService.restore(productId, 1L, null);

        assertThat(stock(productId)).isEqualTo(7L);
        assertThat(historyRepository.count()).isZero();
    }

    @Test
    @DisplayName("수량이 0 이하면 거절한다 (음수 차감으로 재고가 늘어나는 것 방지)")
    void invalidQuantity_rejected() {
        UUID productId = saveProduct(10L);

        assertThatThrownBy(() -> stockService.decrease(productId, -5L, UUID.randomUUID()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> stockService.decrease(productId, 0L, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> stockService.restore(productId, -1L, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(stock(productId)).isEqualTo(10L);
    }

    private long stock(UUID productId) {
        return productRepository.findById(productId).orElseThrow().getStock();
    }

    private UUID saveProduct(long stock) {
        return productRepository.save(Product.builder()
                .companyId(UUID.randomUUID()).hubId(UUID.randomUUID())
                .name("상품-" + UUID.randomUUID()).stock(stock).build()).getId();
    }

    private <T> List<T> runConcurrently(int n, Callable<T> task) throws Exception {
        List<Callable<T>> tasks = new ArrayList<>();
        for (int i = 0; i < n; i++) tasks.add(task);
        return runAll(tasks);
    }

    /** 모든 작업을 한꺼번에 출발시키고 결과를 모은다 (예외가 나면 테스트 실패) */
    private <T> List<T> runAll(List<Callable<T>> tasks) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(tasks.size(), 32));
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger ready = new AtomicInteger();
        try {
            List<Future<T>> futures = new ArrayList<>();
            for (Callable<T> t : tasks) {
                futures.add(pool.submit(() -> {
                    ready.incrementAndGet();
                    start.await();
                    return t.call();
                }));
            }
            start.countDown();
            List<T> results = new ArrayList<>();
            for (Future<T> f : futures) results.add(f.get(60, TimeUnit.SECONDS));
            return results;
        } finally {
            pool.shutdownNow();
        }
    }
}
