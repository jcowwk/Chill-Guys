package com.sparta.logistics.product_service.application.service;

import com.sparta.logistics.product_service.application.dto.response.StockUpdateResponseDto;
import com.sparta.logistics.product_service.domain.entity.StockChangeType;
import com.sparta.logistics.product_service.domain.entity.StockHistory;
import com.sparta.logistics.product_service.domain.repository.ProductRepository;
import com.sparta.logistics.product_service.domain.repository.StockHistoryRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Optional;
import java.util.UUID;

import static com.sparta.logistics.product_service.domain.entity.StockChangeType.DECREASE;
import static com.sparta.logistics.product_service.domain.entity.StockChangeType.RESTORE;

/**
 * 주문 단위 멱등 재고 차감/복구.
 *
 * 주문 ID 가 있으면 (주문, 상품, 유형)별로 한 번만 재고를 바꾸고 결과를 기록한다.
 * - 같은 주문의 차감이 다시 오면: 재고를 다시 깎지 않고 처음 결과를 돌려준다
 * - 복구가 다시 오면: 다시 늘리지 않는다
 * - 차감 기록이 없는 복구(차감 응답이 끊겨 됐는지 모를 때 보낸 보상 등): 재고를 바꾸지 않고 "취소됨"으로 기록,
 *   이후 늦게 도착한 같은 주문의 차감은 거절한다
 * - 복구는 요청 수량이 아니라 실제로 차감했던 수량으로 되돌린다
 *
 * 차감과 복구는 상품 행을 먼저 잠그고 기록을 확인한다. 그래야 같은 주문의 차감·복구가 동시에 와도
 * "확인한 뒤 다른 쪽이 끼어드는" 일이 없다.
 *
 * 주문 ID 가 없는 기존 호출은 이전과 같이 동작한다 (하위 호환).
 */
@Slf4j
@Service
public class StockService {

    private final ProductService productService;
    private final ProductRepository productRepository;
    private final StockHistoryRepository historyRepository;
    private final TransactionTemplate tx;

    public StockService(ProductService productService, ProductRepository productRepository,
                        StockHistoryRepository historyRepository, PlatformTransactionManager transactionManager) {
        this.productService = productService;
        this.productRepository = productRepository;
        this.historyRepository = historyRepository;
        this.tx = new TransactionTemplate(transactionManager);
    }

    public StockUpdateResponseDto decrease(UUID productId, Long quantity, UUID orderId) {
        validate(quantity);
        if (orderId == null) {
            return productService.reduceStock(productId, quantity);
        }
        try {
            return tx.execute(status -> {
                lockProduct(productId);
                Optional<StockHistory> previous = find(orderId, productId, DECREASE);
                if (previous.isPresent()) {
                    return new StockUpdateResponseDto(previous.get().isApplied());
                }
                if (find(orderId, productId, RESTORE).isPresent()) {
                    // 이미 취소(보상)된 주문의 차감이 늦게 도착: 깎지 않는다
                    log.warn("취소된 주문의 늦은 재고 차감 요청 거절 orderId={}, productId={}", orderId, productId);
                    record(orderId, productId, DECREASE, quantity, false);
                    return new StockUpdateResponseDto(false);
                }
                boolean applied = productRepository.decreaseStock(productId, quantity) == 1;
                record(orderId, productId, DECREASE, quantity, applied);
                return new StockUpdateResponseDto(applied);
            });
        } catch (DataIntegrityViolationException e) {
            // 잠금으로 막히지 않은 예외적 동시 요청: 먼저 기록된 결과를 돌려준다 (이쪽 차감은 롤백됨)
            return find(orderId, productId, DECREASE)
                    .map(h -> new StockUpdateResponseDto(h.isApplied()))
                    .orElseThrow(() -> e);
        }
    }

    public StockUpdateResponseDto restore(UUID productId, Long quantity, UUID orderId) {
        validate(quantity);
        if (orderId == null) {
            return productService.addStock(productId, quantity);
        }
        try {
            return tx.execute(status -> {
                lockProduct(productId);
                if (find(orderId, productId, RESTORE).isPresent()) {
                    return new StockUpdateResponseDto(true); // 이미 처리된 복구
                }
                Optional<StockHistory> decrease = find(orderId, productId, DECREASE);
                if (decrease.isPresent() && decrease.get().isApplied()) {
                    Long decreased = decrease.get().getQuantity();
                    productRepository.increaseStock(productId, decreased);
                    record(orderId, productId, RESTORE, decreased, true);
                } else {
                    // 실제로 차감된 적이 없으면 되돌릴 것도 없다. 취소 표시만 남겨 늦은 차감을 막는다
                    record(orderId, productId, RESTORE, quantity, false);
                }
                return new StockUpdateResponseDto(true);
            });
        } catch (DataIntegrityViolationException e) {
            return new StockUpdateResponseDto(true);
        }
    }

    private void lockProduct(UUID productId) {
        if (productRepository.lockById(productId).isEmpty()) {
            throw new RuntimeException("해당 상품 없음");
        }
    }

    private Optional<StockHistory> find(UUID orderId, UUID productId, StockChangeType type) {
        return historyRepository.findByOrderIdAndProductIdAndType(orderId, productId, type);
    }

    private void record(UUID orderId, UUID productId, StockChangeType type, Long quantity, boolean applied) {
        historyRepository.saveAndFlush(new StockHistory(orderId, productId, type, quantity, applied));
    }

    private static void validate(Long quantity) {
        if (quantity == null || quantity < 1) {
            throw new IllegalArgumentException("수량은 1 이상이어야 합니다: " + quantity);
        }
    }
}
