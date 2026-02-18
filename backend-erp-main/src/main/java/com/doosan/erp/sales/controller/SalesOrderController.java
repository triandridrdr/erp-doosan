package com.doosan.erp.sales.controller;

import com.doosan.erp.common.dto.ApiResponse;
import com.doosan.erp.common.dto.PageResponse;
import com.doosan.erp.sales.dto.SalesOrderRequest;
import com.doosan.erp.sales.dto.SalesOrderResponse;
import com.doosan.erp.sales.service.SalesOrderService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 수주 API 컨트롤러
 *
 * 수주(Sales Order) 관련 REST API를 제공하는 컨트롤러입니다.
 * 수주란 고객으로부터 받은 주문을 말하며, ERP 시스템의 영업 모듈 핵심 기능입니다.
 *
 * 제공 기능:
 * - 수주 생성 (POST /api/v1/sales/orders)
 * - 수주 조회 (GET /api/v1/sales/orders/{id})
 * - 수주 목록 조회 (GET /api/v1/sales/orders)
 * - 수주 수정 (PUT /api/v1/sales/orders/{id})
 * - 수주 확정 (POST /api/v1/sales/orders/{id}/confirm)
 * - 수주 삭제 (DELETE /api/v1/sales/orders/{id})
 */
@RestController
@RequestMapping("/api/v1/sales/orders")
@RequiredArgsConstructor
@Tag(name = "Sales - Order", description = "수주 관리 API")
public class SalesOrderController {

    // 수주 비즈니스 로직을 처리하는 서비스
    private final SalesOrderService salesOrderService;

    /**
     * 수주 생성 API
     * 고객 정보와 주문 상품 목록을 받아 새로운 수주를 등록합니다.
     * 생성 성공 시 HTTP 201 상태코드와 함께 수주 정보를 반환합니다.
     */
    @PostMapping
    @Operation(summary = "수주 생성", description = "새로운 수주를 생성합니다")
    public ResponseEntity<ApiResponse<SalesOrderResponse>> createSalesOrder(
            @Valid @RequestBody SalesOrderRequest request) {
        SalesOrderResponse response = salesOrderService.createSalesOrder(request);
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(ApiResponse.success(response, "수주가 생성되었습니다"));
    }

    /**
     * 수주 상세 조회 API
     * 수주 ID로 해당 수주의 상세 정보를 조회합니다.
     * 수주 헤더 정보와 함께 모든 라인 정보도 포함됩니다.
     */
    @GetMapping("/{id}")
    @Operation(summary = "수주 조회", description = "수주 상세 정보를 조회합니다")
    public ResponseEntity<ApiResponse<SalesOrderResponse>> getSalesOrder(@PathVariable Long id) {
        SalesOrderResponse response = salesOrderService.getSalesOrder(id);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    /**
     * 수주 목록 조회 API
     * 페이징 처리된 수주 목록을 조회합니다.
     * page와 size 파라미터로 페이지네이션을 제어할 수 있습니다.
     */
    @GetMapping
    @Operation(summary = "수주 목록 조회", description = "수주 목록을 페이징하여 조회합니다")
    public ResponseEntity<ApiResponse<PageResponse<SalesOrderResponse>>> getSalesOrders(
            @RequestParam(name = "page", defaultValue = "0") int page,
            @RequestParam(name = "size", defaultValue = "20") int size) {
        PageResponse<SalesOrderResponse> response = salesOrderService.getSalesOrders(page, size);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    /**
     * 수주 수정 API
     * 기존 수주의 정보를 수정합니다.
     * 확정된 수주는 수정할 수 없으니 주의하세요.
     */
    @PutMapping("/{id}")
    @Operation(summary = "수주 수정", description = "수주 정보를 수정합니다")
    public ResponseEntity<ApiResponse<SalesOrderResponse>> updateSalesOrder(
            @PathVariable Long id,
            @Valid @RequestBody SalesOrderRequest request) {
        SalesOrderResponse response = salesOrderService.updateSalesOrder(id, request);
        return ResponseEntity.ok(ApiResponse.success(response, "수주가 수정되었습니다"));
    }

    /**
     * 수주 확정 API
     * 대기 상태의 수주를 확정 처리합니다.
     * 확정된 수주는 더 이상 수정할 수 없습니다.
     */
    @PostMapping("/{id}/confirm")
    @Operation(summary = "수주 확정", description = "수주를 확정 처리합니다")
    public ResponseEntity<ApiResponse<SalesOrderResponse>> confirmSalesOrder(@PathVariable Long id) {
        SalesOrderResponse response = salesOrderService.confirmSalesOrder(id);
        return ResponseEntity.ok(ApiResponse.success(response, "수주가 확정되었습니다"));
    }

    /**
     * 수주 삭제 API
     * 수주를 삭제합니다. Soft Delete 방식으로 실제 데이터는 유지됩니다.
     * 출하완료된 수주는 삭제할 수 없습니다.
     */
    @DeleteMapping("/{id}")
    @Operation(summary = "수주 삭제", description = "수주를 삭제합니다 (Soft Delete)")
    public ResponseEntity<ApiResponse<Void>> deleteSalesOrder(@PathVariable Long id) {
        salesOrderService.deleteSalesOrder(id);
        return ResponseEntity.ok(ApiResponse.success(null, "수주가 삭제되었습니다"));
    }
}
