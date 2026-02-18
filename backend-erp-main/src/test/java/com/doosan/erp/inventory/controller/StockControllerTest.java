package com.doosan.erp.inventory.controller;

import com.doosan.erp.inventory.dto.StockResponse;
import com.doosan.erp.inventory.service.StockService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 재고 Controller 테스트
 */
@WebMvcTest(controllers = StockController.class, excludeAutoConfiguration = {
                org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration.class,
                org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class,
                org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration.class
})
class StockControllerTest {

        @Autowired
        private MockMvc mockMvc;

        @MockitoBean
        private StockService stockService;

        @Test
        @DisplayName("재고 조회 성공")
        void getStock_Success() throws Exception {
                // Given
                Long stockId = 1L;
                StockResponse response = new StockResponse(
                                stockId, "ITEM-001", "노트북", "WH-001", "중앙창고",
                                BigDecimal.valueOf(100), BigDecimal.valueOf(90), BigDecimal.TEN,
                                "EA", BigDecimal.valueOf(1000000), null, "admin");

                when(stockService.getStock(stockId)).thenReturn(response);

                // When & Then
                mockMvc.perform(get("/api/v1/inventory/stocks/{id}", stockId))
                                .andDo(print())
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.success").value(true))
                                .andExpect(jsonPath("$.data.id").value(stockId))
                                .andExpect(jsonPath("$.data.itemCode").value("ITEM-001"))
                                .andExpect(jsonPath("$.data.itemName").value("노트북"));
        }

        @Test
        @DisplayName("전체 재고 조회 성공")
        void getAllStocks_Success() throws Exception {
                // Given
                StockResponse response1 = new StockResponse(
                                1L, "ITEM-001", "노트북", "WH-001", "중앙창고",
                                BigDecimal.valueOf(100), BigDecimal.valueOf(90), BigDecimal.TEN,
                                "EA", BigDecimal.valueOf(1000000), null, "admin");
                StockResponse response2 = new StockResponse(
                                2L, "ITEM-002", "마우스", "WH-001", "중앙창고",
                                BigDecimal.valueOf(200), BigDecimal.valueOf(200), BigDecimal.ZERO,
                                "EA", BigDecimal.valueOf(50000), null, "admin");

                when(stockService.getAllStocks()).thenReturn(List.of(response1, response2));

                // When & Then
                mockMvc.perform(get("/api/v1/inventory/stocks"))
                                .andDo(print())
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.success").value(true))
                                .andExpect(jsonPath("$.data").isArray())
                                .andExpect(jsonPath("$.data.length()").value(2))
                                .andExpect(jsonPath("$.data[0].itemCode").value("ITEM-001"))
                                .andExpect(jsonPath("$.data[1].itemCode").value("ITEM-002"));
        }

        @Test
        @DisplayName("품목별 재고 조회 성공")
        void getStocksByItemCode_Success() throws Exception {
                // Given
                String itemCode = "ITEM-001";
                StockResponse response = new StockResponse(
                                1L, itemCode, "노트북", "WH-001", "중앙창고",
                                BigDecimal.valueOf(100), BigDecimal.valueOf(90), BigDecimal.TEN,
                                "EA", BigDecimal.valueOf(1000000), null, "admin");

                when(stockService.getStocksByItemCode(itemCode)).thenReturn(List.of(response));

                // When & Then
                mockMvc.perform(get("/api/v1/inventory/stocks/item/{itemCode}", itemCode))
                                .andDo(print())
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.success").value(true))
                                .andExpect(jsonPath("$.data").isArray())
                                .andExpect(jsonPath("$.data[0].itemCode").value(itemCode));
        }

        @Test
        @DisplayName("창고별 재고 조회 성공")
        void getStocksByWarehouseCode_Success() throws Exception {
                // Given
                String warehouseCode = "WH-001";
                StockResponse response1 = new StockResponse(
                                1L, "ITEM-001", "노트북", warehouseCode, "중앙창고",
                                BigDecimal.valueOf(100), BigDecimal.valueOf(90), BigDecimal.TEN,
                                "EA", BigDecimal.valueOf(1000000), null, "admin");
                StockResponse response2 = new StockResponse(
                                2L, "ITEM-002", "마우스", warehouseCode, "중앙창고",
                                BigDecimal.valueOf(200), BigDecimal.valueOf(200), BigDecimal.ZERO,
                                "EA", BigDecimal.valueOf(50000), null, "admin");

                when(stockService.getStocksByWarehouseCode(warehouseCode)).thenReturn(List.of(response1, response2));

                // When & Then
                mockMvc.perform(get("/api/v1/inventory/stocks/warehouse/{warehouseCode}", warehouseCode))
                                .andDo(print())
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.success").value(true))
                                .andExpect(jsonPath("$.data").isArray())
                                .andExpect(jsonPath("$.data.length()").value(2))
                                .andExpect(jsonPath("$.data[0].warehouseCode").value(warehouseCode))
                                .andExpect(jsonPath("$.data[1].warehouseCode").value(warehouseCode));
        }
}
