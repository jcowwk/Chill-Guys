package com.sparta.logistics.product_service.domain.repository;

import com.sparta.logistics.product_service.domain.entity.Product;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface ProductRepository extends JpaRepository<Product, UUID> {
    @Query("SELECT p FROM Product p WHERE " +
            "(:name IS NULL OR p.name LIKE %:name%) AND " +
            "(:companyId IS NULL OR p.companyId = :companyId) AND " +
            "(:hubId IS NULL OR p.hubId = :hubId)")
    Page<Product> searchProducts(@Param("name") String name,
                                 @Param("companyId") UUID companyId,
                                 @Param("hubId") UUID hubId,
                                 Pageable pageable);

    Optional<Product> findByName(String name);

    Product findByIdAndDeletedAtIsNull(UUID productId);

    // 재고 확인과 차감을 UPDATE 한 문장으로 처리해, 동시 주문 시 초과 판매와 차감 유실을 막는다.
    // 재고가 부족하면 조건에 걸려 0건이 갱신된다.
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE Product p SET p.stock = p.stock - :quantity, p.updatedAt = CURRENT_TIMESTAMP " +
            "WHERE p.id = :productId AND p.stock >= :quantity")
    int decreaseStock(@Param("productId") UUID productId, @Param("quantity") Long quantity);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE Product p SET p.stock = p.stock + :quantity, p.updatedAt = CURRENT_TIMESTAMP " +
            "WHERE p.id = :productId")
    int increaseStock(@Param("productId") UUID productId, @Param("quantity") Long quantity);
}
