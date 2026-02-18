package com.doosan.erp.inventory.controller;

import com.doosan.erp.common.dto.ApiResponse;
import com.doosan.erp.inventory.dto.StockRequest;
import com.doosan.erp.inventory.dto.StockResponse;
import com.doosan.erp.inventory.service.StockService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 재고 API 컨트롤러
 *
 * 재고(Stock) 관련 REST API를 제공하는 컨트롤러입니다.
 * 재고는 품목별, 창고별로 관리되며, 수량, 가용 수량, 예약 수량을 추적합니다.
 *
 * 제공 기능:
 * - 재고 생성 (POST /api/v1/inventory/stocks)
 * - 재고 조회 (GET /api/v1/inventory/stocks/{id})
 * - 전체 재고 조회 (GET /api/v1/inventory/stocks)
 * - 품목별 재고 조회 (GET /api/v1/inventory/stocks/item/{itemCode})
 * - 창고별 재고 조회 (GET /api/v1/inventory/stocks/warehouse/{warehouseCode})
 */
@RestController
@RequestMapping("/api/v1/inventory/stocks")
@RequiredArgsConstructor
@Tag(name = "Inventory - Stock", description = "재고 관리 API")
public class StockController {

    // 재고 비즈니스 로직을 처리하는 서비스
    private final StockService stockService;

    /**
     * 재고 생성 API
     *
     * 새로운 재고를 등록합니다.
     * 품목코드와 창고코드 조합이 이미 존재하면 예외가 발생합니다.
     * 생성 성공 시 HTTP 201 상태코드와 함께 재고 정보를 반환합니다.
     *
     * @param request 재고 생성 요청 정보 (품목코드, 품목명, 창고코드, 창고명, 수량, 단위, 단가)
     * @return 생성된 재고 정보
     */
    @PostMapping
    @Operation(summary = "재고 생성", description = "새로운 재고를 생성합니다")
    public ResponseEntity<ApiResponse<StockResponse>> createStock(
            @Valid @RequestBody StockRequest request) {
        StockResponse response = stockService.createStock(request);
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(ApiResponse.success(response, "재고가 생성되었습니다"));
    }

    /**
     * 재고 상세 조회 API
     *
     * 재고 ID로 해당 재고의 상세 정보를 조회합니다.
     * 총 수량, 가용 수량, 예약 수량 등 모든 재고 정보가 포함됩니다.
     *
     * @param id 재고 ID
     * @return 재고 상세 정보
     */
    @GetMapping("/{id}")
    @Operation(summary = "재고 조회", description = "재고 상세 정보를 조회합니다")
    public ResponseEntity<ApiResponse<StockResponse>> getStock(@PathVariable Long id) {
        StockResponse response = stockService.getStock(id);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    /**
     * 전체 재고 조회 API
     *
     * 삭제되지 않은 모든 재고를 조회합니다.
     * 품목별, 창고별로 그룹화되지 않은 전체 목록을 반환합니다.
     *
     * @return 전체 재고 목록
     */
    @GetMapping
    @Operation(summary = "전체 재고 조회", description = "전체 재고 현황을 조회합니다")
    public ResponseEntity<ApiResponse<List<StockResponse>>> getAllStocks() {
        List<StockResponse> response = stockService.getAllStocks();
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    /**
     * 품목별 재고 조회 API
     *
     * 특정 품목의 모든 창고별 재고 현황을 조회합니다.
     * 동일 품목이 여러 창고에 보관된 경우 모두 조회됩니다.
     *
     * @param itemCode 품목 코드
     * @return 해당 품목의 재고 목록
     */
    @GetMapping("/item/{itemCode}")
    @Operation(summary = "품목별 재고 조회", description = "특정 품목의 재고 현황을 조회합니다")
    public ResponseEntity<ApiResponse<List<StockResponse>>> getStocksByItemCode(
            @PathVariable String itemCode) {
        List<StockResponse> response = stockService.getStocksByItemCode(itemCode);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    /**
     * 창고별 재고 조회 API
     *
     * 특정 창고의 모든 품목별 재고 현황을 조회합니다.
     * 해당 창고에 보관된 모든 품목의 재고가 조회됩니다.
     *
     * @param warehouseCode 창고 코드
     * @return 해당 창고의 재고 목록
     */
    @GetMapping("/warehouse/{warehouseCode}")
    @Operation(summary = "창고별 재고 조회", description = "특정 창고의 재고 현황을 조회합니다")
    public ResponseEntity<ApiResponse<List<StockResponse>>> getStocksByWarehouseCode(
            @PathVariable String warehouseCode) {
        List<StockResponse> response = stockService.getStocksByWarehouseCode(warehouseCode);
        return ResponseEntity.ok(ApiResponse.success(response));
    }
}
