package com.sparta.logistics.product_service.application.service;

import com.sparta.logistics.product_service.domain.entity.Product;
import com.sparta.logistics.product_service.domain.repository.ProductRepository;
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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 여러 주문이 같은 상품의 재고를 동시에 차감/증가할 때 재고가 정확히 유지되는지 검증.
 * 실제 행 잠금 동작을 확인해야 하므로 PostgreSQL 컨테이너를 사용한다.
 */
@DataJpaTest
@Testcontainers
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(ProductService.class)
@Transactional(propagation = Propagation.NOT_SUPPORTED) // 각 스레드가 자기 트랜잭션으로 커밋해야 하므로 테스트 트랜잭션 비활성화
class ProductStockConcurrencyTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private ProductService productService;

    @Autowired
    private ProductRepository productRepository;

    @MockitoBean
    private CompanyClient companyClient;

    @MockitoBean
    private HubClient hubClient;

    @AfterEach
    void tearDown() {
        productRepository.deleteAll();
    }

    @Test
    @DisplayName("재고 100개에 200건 동시 차감 → 정확히 100건만 성공하고 재고는 0")
    void concurrentDecrease_noOversellAndNoLostUpdate() throws InterruptedException {
        UUID productId = saveProduct(100L);

        AtomicInteger success = new AtomicInteger();
        runConcurrently(200, () -> {
            if (Boolean.TRUE.equals(productService.reduceStock(productId, 1L).getSuccess())) {
                success.incrementAndGet();
            }
        });

        long stock = productRepository.findById(productId).orElseThrow().getStock();
        System.out.printf("[decrease] success=%d, remaining stock=%d%n", success.get(), stock);
        assertThat(success.get()).isEqualTo(100);
        assertThat(stock).isZero();
    }

    @Test
    @DisplayName("재고 0개에 100건 동시 증가 → 재고 100 (증가분 유실 없음)")
    void concurrentIncrease_noLostUpdate() throws InterruptedException {
        UUID productId = saveProduct(0L);

        runConcurrently(100, () -> productService.addStock(productId, 1L));

        long stock = productRepository.findById(productId).orElseThrow().getStock();
        System.out.printf("[increase] remaining stock=%d%n", stock);
        assertThat(stock).isEqualTo(100L);
    }

    @Test
    @DisplayName("재고보다 많은 수량 차감 요청은 실패하고 재고는 그대로")
    void decreaseMoreThanStock_fails() {
        UUID productId = saveProduct(5L);

        boolean result = productService.reduceStock(productId, 6L).getSuccess();

        assertThat(result).isFalse();
        assertThat(productRepository.findById(productId).orElseThrow().getStock()).isEqualTo(5L);
    }

    private UUID saveProduct(long stock) {
        return productRepository.save(Product.builder()
                .companyId(UUID.randomUUID())
                .hubId(UUID.randomUUID())
                .name("상품-" + UUID.randomUUID())
                .stock(stock)
                .build()).getId();
    }

    /** start gate 로 모든 스레드를 동시에 출발시킨다. */
    private void runConcurrently(int requests, Runnable task) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(requests);
        CountDownLatch ready = new CountDownLatch(requests);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(requests);
        for (int i = 0; i < requests; i++) {
            executor.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    task.run();
                } catch (Exception ignored) {
                    // 실패한 요청은 success 로 집계되지 않는다
                } finally {
                    done.countDown();
                }
            });
        }
        ready.await(5, TimeUnit.SECONDS);
        start.countDown();
        done.await(60, TimeUnit.SECONDS);
        executor.shutdown();
    }
}
