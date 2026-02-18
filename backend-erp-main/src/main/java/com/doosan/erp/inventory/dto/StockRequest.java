package com.doosan.erp.inventory.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 재고 생성 요청 DTO
 *
 * 재고 생성 API 요청 시 클라이언트가 전송하는 데이터입니다.
 * Bean Validation을 통해 필수값 및 양수 검증을 수행합니다.
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class StockRequest {

    // 품목 코드 (필수)
    @NotBlank(message = "품목 코드는 필수입니다")
    private String itemCode;

    // 품목명 (필수)
    @NotBlank(message = "품목명은 필수입니다")
    private String itemName;

    // 창고 코드 (필수)
    @NotBlank(message = "창고 코드는 필수입니다")
    private String warehouseCode;

    // 창고명 (필수)
    @NotBlank(message = "창고명은 필수입니다")
    private String warehouseName;

    // 재고 수량 (필수, 양수)
    @NotNull(message = "수량은 필수입니다")
    @Positive(message = "수량은 0보다 커야 합니다")
    private BigDecimal quantity;

    // 단위 (필수, 예: 개, 박스, kg)
    @NotBlank(message = "단위는 필수입니다")
    private String unit;

    // 단가 (선택, 양수)
    @Positive(message = "단가는 0보다 커야 합니다")
    private BigDecimal unitPrice;
}
