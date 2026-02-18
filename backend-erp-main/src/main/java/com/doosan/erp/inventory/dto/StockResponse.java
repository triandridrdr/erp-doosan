package com.doosan.erp.inventory.dto;

import com.doosan.erp.inventory.entity.Stock;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 재고 응답 DTO
 *
 * 재고 조회 API 응답 시 클라이언트에게 반환하는 데이터입니다.
 * 엔티티를 DTO로 변환하는 from() 정적 메서드를 제공합니다.
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class StockResponse {

    // 재고 ID
    private Long id;

    // 품목 코드
    private String itemCode;

    // 품목명
    private String itemName;

    // 창고 코드
    private String warehouseCode;

    // 창고명
    private String warehouseName;

    // 총 재고 수량
    private BigDecimal quantity;

    // 가용 재고 수량
    private BigDecimal availableQuantity;

    // 예약된 재고 수량
    private BigDecimal reservedQuantity;

    // 단위
    private String unit;

    // 단가
    private BigDecimal unitPrice;

    // 생성일시
    private LocalDateTime createdAt;

    // 생성자
    private String createdBy;

    /**
     * 엔티티를 응답 DTO로 변환
     *
     * @param entity 변환할 재고 엔티티
     * @return 변환된 응답 DTO
     */
    public static StockResponse from(Stock entity) {
        return new StockResponse(
                entity.getId(),
                entity.getItemCode(),
                entity.getItemName(),
                entity.getWarehouseCode(),
                entity.getWarehouseName(),
                entity.getQuantity(),
                entity.getAvailableQuantity(),
                entity.getReservedQuantity(),
                entity.getUnit(),
                entity.getUnitPrice(),
                entity.getCreatedAt(),
                entity.getCreatedBy());
    }
}
