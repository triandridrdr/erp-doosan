package com.doosan.erp.sales.entity;

import com.doosan.erp.common.entity.BaseEntity;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

/**
 * 수주 라인 엔티티
 *
 * 수주에 포함된 개별 품목(상품) 정보를 저장합니다.
 * 하나의 수주(SalesOrder)에 여러 개의 라인이 포함될 수 있습니다 (N:1 관계).
 *
 * 테이블: sales_order_lines
 *
 * 주요 필드:
 * - lineNumber: 라인 번호 (수주 내 순서)
 * - itemCode, itemName: 품목 정보
 * - quantity: 주문 수량
 * - unitPrice: 단가
 * - lineAmount: 라인 금액 (수량 * 단가, 자동 계산)
 *
 * BaseEntity를 상속하여 ID, 생성일시, 수정일시 등을 제공받습니다.
 */
@Entity
@Table(name = "sales_order_lines")
@Getter
@Setter
@NoArgsConstructor
public class SalesOrderLine extends BaseEntity {

    // 상위 수주 엔티티와의 N:1 관계
    // LAZY 로딩으로 성능 최적화
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "sales_order_id", nullable = false)
    private SalesOrder salesOrder;

    // 라인 번호: 수주 내에서의 순서
    @Column(name = "line_number", nullable = false)
    private Integer lineNumber;

    // 품목코드: 품목 마스터의 코드 참조
    @Column(name = "item_code", nullable = false, length = 50)
    private String itemCode;

    // 품목명
    @Column(name = "item_name", nullable = false, length = 200)
    private String itemName;

    // 주문 수량
    @Column(name = "quantity", precision = 19, scale = 2, nullable = false)
    private BigDecimal quantity;

    // 단가
    @Column(name = "unit_price", precision = 19, scale = 2, nullable = false)
    private BigDecimal unitPrice;

    // 라인 금액: 수량 * 단가 (자동 계산)
    @Column(name = "line_amount", precision = 19, scale = 2, nullable = false)
    private BigDecimal lineAmount;

    // 비고
    @Column(name = "remarks", length = 500)
    private String remarks;

    /**
     * 수주 라인 생성자
     *
     * 라인 생성 시 수량과 단가를 곱하여 라인 금액을 자동 계산합니다.
     *
     * @param lineNumber 라인 번호
     * @param itemCode 품목코드
     * @param itemName 품목명
     * @param quantity 수량
     * @param unitPrice 단가
     * @param remarks 비고
     */
    public SalesOrderLine(Integer lineNumber, String itemCode, String itemName,
                         BigDecimal quantity, BigDecimal unitPrice, String remarks) {
        this.lineNumber = lineNumber;
        this.itemCode = itemCode;
        this.itemName = itemName;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.lineAmount = quantity.multiply(unitPrice);
        this.remarks = remarks;
    }

    /**
     * 라인 금액 재계산
     *
     * 수량이나 단가가 변경되었을 때 호출하여 라인 금액을 다시 계산합니다.
     * 라인 금액 = 수량 * 단가
     */
    public void recalculateLineAmount() {
        this.lineAmount = this.quantity.multiply(this.unitPrice);
    }
}
