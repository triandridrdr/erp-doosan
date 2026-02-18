package com.doosan.erp.sales.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

/**
 * 수주 생성/수정 요청 DTO
 *
 * 수주 생성 또는 수정 API 요청 시 클라이언트가 전송하는 데이터입니다.
 * Bean Validation을 통해 필수값 검증을 수행합니다.
 *
 * 수주 헤더 정보와 라인(상품) 목록을 포함합니다.
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class SalesOrderRequest {

    // 수주일자 (필수)
    @NotNull(message = "수주일자는 필수입니다")
    private LocalDate orderDate;

    // 고객코드 (필수)
    @NotBlank(message = "고객코드는 필수입니다")
    private String customerCode;

    // 고객명 (필수)
    @NotBlank(message = "고객명은 필수입니다")
    private String customerName;

    // 배송지 주소 (선택)
    private String deliveryAddress;

    // 비고 (선택)
    private String remarks;

    // 수주 라인 목록 (최소 1개 이상 필수)
    // @Valid: 중첩된 객체도 검증 수행
    @NotEmpty(message = "수주 라인은 최소 1개 이상 필요합니다")
    @Valid
    private List<SalesOrderLineRequest> lines;

    /**
     * 수주 라인 요청 DTO
     *
     * 개별 품목(상품)에 대한 주문 정보입니다.
     * 품목코드, 품목명, 수량, 단가는 필수 입력 항목입니다.
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SalesOrderLineRequest {

        // 라인 번호 (필수): 수주 내 순서
        @NotNull(message = "라인번호는 필수입니다")
        private Integer lineNumber;

        // 품목코드 (필수)
        @NotBlank(message = "품목코드는 필수입니다")
        private String itemCode;

        // 품목명 (필수)
        @NotBlank(message = "품목명은 필수입니다")
        private String itemName;

        // 주문 수량 (필수)
        @NotNull(message = "수량은 필수입니다")
        private BigDecimal quantity;

        // 단가 (필수)
        @NotNull(message = "단가는 필수입니다")
        private BigDecimal unitPrice;

        // 비고 (선택)
        private String remarks;
    }
}
