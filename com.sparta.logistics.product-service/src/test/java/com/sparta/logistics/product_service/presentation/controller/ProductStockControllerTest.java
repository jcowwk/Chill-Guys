package com.sparta.logistics.product_service.presentation.controller;

import com.sparta.logistics.product_service.application.dto.response.StockUpdateResponseDto;
import com.sparta.logistics.product_service.application.service.ProductService;
import com.sparta.logistics.product_service.application.service.StockService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 재고 API 가 주문 ID 를 받아 전달하는지, 기존 형식 요청도 받는지 확인 */
class ProductStockControllerTest {

    private StockService stockService;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        stockService = mock(StockService.class);
        mockMvc = MockMvcBuilders.standaloneSetup(new ProductController(mock(ProductService.class), stockService)).build();
    }

    @Test
    void 차감_요청의_orderId_를_전달한다() throws Exception {
        UUID productId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        when(stockService.decrease(eq(productId), eq(3L), eq(orderId))).thenReturn(new StockUpdateResponseDto(true));

        mockMvc.perform(post("/api/products/{id}/decrease-stock", productId).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"productId\":\"" + productId + "\",\"quantity\":3,\"orderId\":\"" + orderId + "\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));
    }

    @Test
    void orderId_없는_기존_형식_요청도_받는다() throws Exception {
        UUID productId = UUID.randomUUID();
        when(stockService.decrease(eq(productId), eq(3L), isNull())).thenReturn(new StockUpdateResponseDto(true));

        mockMvc.perform(post("/api/products/{id}/decrease-stock", productId).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"productId\":\"" + productId + "\",\"quantity\":3}"))
                .andExpect(status().isOk());
        verify(stockService).decrease(eq(productId), eq(3L), isNull());
    }

    @Test
    void 복구_요청의_orderId_파라미터를_전달한다() throws Exception {
        UUID productId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        when(stockService.restore(any(), any(), any())).thenReturn(new StockUpdateResponseDto(true));

        mockMvc.perform(post("/api/products/{id}/increase-stock", productId)
                        .param("quantity", "3").param("orderId", orderId.toString()))
                .andExpect(status().isOk());
        verify(stockService).restore(productId, 3L, orderId);
    }

    @Test
    void 잘못된_수량은_400() throws Exception {
        when(stockService.decrease(any(), any(), any())).thenThrow(new IllegalArgumentException("수량은 1 이상이어야 합니다: -1"));

        mockMvc.perform(post("/api/products/{id}/decrease-stock", UUID.randomUUID()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"quantity\":-1}"))
                .andExpect(status().isBadRequest());
    }
}
