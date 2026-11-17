package com.sparta.logistics.product_service.presentation.controller;

import com.sparta.logistics.product_service.application.dto.request.ProductCreateRequestDto;
import com.sparta.logistics.product_service.application.dto.request.ProductStockRequestDto;
import com.sparta.logistics.product_service.application.dto.request.ProductUpdateRequestDto;
import com.sparta.logistics.product_service.application.dto.response.ProductCreateResponseDto;
import com.sparta.logistics.product_service.application.dto.response.ProductGetResponseDto;
import com.sparta.logistics.product_service.application.dto.response.ProductSearchResponseDto;
import com.sparta.logistics.product_service.application.dto.response.ProductUpdateResponseDto;
import com.sparta.logistics.product_service.application.dto.response.StockUpdateResponseDto;
import com.sparta.logistics.product_service.application.service.ProductService;
import com.sparta.logistics.product_service.application.service.StockService;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping({"/api/products"})
public class ProductController {
    private final ProductService productService;
    private final StockService stockService;

    public ProductController(ProductService productService, StockService stockService) {
        this.productService = productService;
        this.stockService = stockService;
    }

    @PostMapping
    public ProductCreateResponseDto createProduct(@RequestBody ProductCreateRequestDto requestProductCreateDto, @RequestHeader(value = "X-User-Id") String userIdHeader) {
        return this.productService.createProduct(requestProductCreateDto, userIdHeader);
    }

    @PutMapping({"/{productId}"})
    public ProductUpdateResponseDto updateProduct(@PathVariable("productId") UUID productId, @RequestBody ProductUpdateRequestDto requestProductUpdateDto, @RequestHeader(value = "X-User-Id") String userIdHeader) {
        return this.productService.updateProduct(productId, requestProductUpdateDto, userIdHeader);
    }

    @GetMapping({"/search"})
    public Page<ProductSearchResponseDto> searchProducts(@RequestParam(required = false) String name, @RequestParam(required = false) UUID companyId, @RequestParam(required = false) UUID hubId, @RequestParam(required = false,defaultValue = "1") int page, @RequestParam(required = false,defaultValue = "10") int size, @RequestParam(required = false,defaultValue = "createdAt") String sortBy, @RequestParam(required = false,defaultValue = "asc") String order) {
        return this.productService.searchProducts(name, companyId, hubId, page - 1, size, sortBy, order);
    }

    @GetMapping({"/{productId}"})
    public ProductGetResponseDto getProduct(@PathVariable("productId") UUID productId) {
        return this.productService.getProduct(productId);
    }

    @GetMapping
    public Page<ProductSearchResponseDto> getAllProducts(@RequestParam(required = false,defaultValue = "1") int page, @RequestParam(required = false,defaultValue = "10") int size, @RequestParam(required = false,defaultValue = "createdAt") String sortBy, @RequestParam(required = false,defaultValue = "asc") String order) {
        return this.productService.getAllProducts(page - 1, size, sortBy, order);
    }

    @DeleteMapping({"/{productId}"})
    public ResponseEntity<Void> deleteProduct(@PathVariable("productId") UUID productId, @RequestHeader(value = "X-User-Id") String userIdHeader) {
        return this.productService.deleteProduct(productId, userIdHeader);
    }

    @PostMapping({"/{productId}/decrease-stock"})
    public ResponseEntity<StockUpdateResponseDto> decreaseStock(@PathVariable UUID productId, @RequestBody ProductStockRequestDto requestDto) {
        StockUpdateResponseDto response = this.stockService.decrease(productId, requestDto.getQuantity(), requestDto.getOrderId());
        return ResponseEntity.ok(response);
    }

    @PostMapping({"/{productId}/increase-stock"})
    public ResponseEntity<StockUpdateResponseDto> increaseStock(@PathVariable UUID productId, @RequestParam Long quantity,
                                                                @RequestParam(required = false) UUID orderId) {
        StockUpdateResponseDto response = this.stockService.restore(productId, quantity, orderId);
        return ResponseEntity.ok(response);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<java.util.Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(java.util.Map.of("message", e.getMessage()));
    }

    @GetMapping({"/{productId}/hub"})
    public ResponseEntity<UUID> getHubIdByProductId(@PathVariable("productId") UUID productId) {
        UUID hubId = this.productService.getHubIdByProductId(productId);
        return ResponseEntity.ok(hubId);
    }

    @GetMapping("/{productId}/name")
    public String getProductName(@PathVariable("productId") UUID productId) {
        return productService.getProductName(productId);
    }
}
