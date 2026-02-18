package com.doosan.erp.sales.controller;

import com.doosan.erp.sales.dto.SalesOrderRequest;
import com.doosan.erp.sales.dto.SalesOrderResponse;
import com.doosan.erp.sales.service.SalesOrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 수주 Controller 테스트
 */
@WebMvcTest(controllers = SalesOrderController.class, excludeAutoConfiguration = {
                org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration.class,
                org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class,
                org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration.class
})
class SalesOrderControllerTest {

        @Autowired
        private MockMvc mockMvc;

        @Autowired
        private ObjectMapper objectMapper;

        @MockitoBean
        private SalesOrderService salesOrderService;

        @Test
        @DisplayName("수주 생성 성공")
        void createSalesOrder_Success() throws Exception {
                // Given
                SalesOrderRequest.SalesOrderLineRequest lineRequest = new SalesOrderRequest.SalesOrderLineRequest(
                                1, "ITEM-001", "노트북", BigDecimal.TEN, BigDecimal.valueOf(1000000), "테스트");

                SalesOrderRequest request = new SalesOrderRequest(
                                LocalDate.now(),
                                "CUST-001",
                                "테스트 고객",
                                "서울시 강남구",
                                "비고",
                                List.of(lineRequest));

                SalesOrderResponse response = new SalesOrderResponse(
                                1L, "SO-2025-0001", LocalDate.now(), "CUST-001", "테스트 고객",
                                "PENDING", BigDecimal.valueOf(10000000), "서울시 강남구", "비고",
                                List.of(), null, "admin");

                when(salesOrderService.createSalesOrder(any())).thenReturn(response);

                // When & Then
                mockMvc.perform(post("/api/v1/sales/orders")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andDo(print())
                                .andExpect(status().isCreated())
                                .andExpect(jsonPath("$.success").value(true))
                                .andExpect(jsonPath("$.data.orderNumber").value("SO-2025-0001"));
        }

        @Test
        @DisplayName("수주 조회 성공")
        void getSalesOrder_Success() throws Exception {
                // Given
                Long orderId = 1L;
                SalesOrderResponse response = new SalesOrderResponse(
                                orderId, "SO-2025-0001", LocalDate.now(), "CUST-001", "테스트 고객",
                                "PENDING", BigDecimal.valueOf(10000000), "서울시 강남구", "비고",
                                List.of(), null, "admin");

                when(salesOrderService.getSalesOrder(orderId)).thenReturn(response);

                // When & Then
                mockMvc.perform(get("/api/v1/sales/orders/{id}", orderId))
                                .andDo(print())
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.success").value(true))
                                .andExpect(jsonPath("$.data.id").value(orderId))
                                .andExpect(jsonPath("$.data.orderNumber").value("SO-2025-0001"));
        }
}
