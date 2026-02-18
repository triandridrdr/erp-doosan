package com.doosan.erp.sales.dto;

import com.doosan.erp.sales.entity.SalesOrder;
import com.doosan.erp.sales.entity.SalesOrderLine;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 수주 응답 DTO
 *
 * 수주 조회 API 응답 시 클라이언트에게 반환되는 데이터입니다.
 * 수주 헤더 정보와 라인 목록을 포함합니다.
 *
 * Entity를 직접 노출하지 않고 DTO로 변환하여 반환함으로써
 * API 계약과 도메인 모델을 분리합니다.
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class SalesOrderResponse {

    private Long id;                  // 수주 ID
    private String orderNumber;       // 수주번호
    private LocalDate orderDate;      // 수주일자
    private String customerCode;      // 고객코드
    private String customerName;      // 고객명
    private String status;            // 수주 상태 (PENDING/CONFIRMED/SHIPPED/CANCELLED)
    private BigDecimal totalAmount;   // 총 금액
    private String deliveryAddress;   // 배송지 주소
    private String remarks;           // 비고
    private List<SalesOrderLineResponse> lines;  // 수주 라인 목록
    private LocalDateTime createdAt;  // 생성일시
    private String createdBy;         // 생성자

    /**
     * Entity -> DTO 변환 팩토리 메서드
     *
     * 수주 엔티티를 응답 DTO로 변환합니다.
     * 라인 목록도 함께 변환합니다.
     *
     * @param entity 수주 엔티티
     * @return 수주 응답 DTO
     */
    public static SalesOrderResponse from(SalesOrder entity) {
        return new SalesOrderResponse(
                entity.getId(),
                entity.getOrderNumber(),
                entity.getOrderDate(),
                entity.getCustomerCode(),
                entity.getCustomerName(),
                entity.getStatus().name(),
                entity.getTotalAmount(),
                entity.getDeliveryAddress(),
                entity.getRemarks(),
                entity.getLines().stream()
                        .map(SalesOrderLineResponse::from)
                        .collect(Collectors.toList()),
                entity.getCreatedAt(),
                entity.getCreatedBy()
        );
    }

    /**
     * 수주 라인 응답 DTO
     *
     * 개별 품목에 대한 주문 정보를 담고 있습니다.
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SalesOrderLineResponse {
        private Long id;              // 라인 ID
        private Integer lineNumber;   // 라인 번호
        private String itemCode;      // 품목코드
        private String itemName;      // 품목명
        private BigDecimal quantity;  // 수량
        private BigDecimal unitPrice; // 단가
        private BigDecimal lineAmount;// 라인 금액 (수량 * 단가)
        private String remarks;       // 비고

        /**
         * Entity -> DTO 변환 팩토리 메서드
         */
        public static SalesOrderLineResponse from(SalesOrderLine entity) {
            return new SalesOrderLineResponse(
                    entity.getId(),
                    entity.getLineNumber(),
                    entity.getItemCode(),
                    entity.getItemName(),
                    entity.getQuantity(),
                    entity.getUnitPrice(),
                    entity.getLineAmount(),
                    entity.getRemarks()
            );
        }
    }
}
