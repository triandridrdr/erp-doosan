package com.doosan.erp.inventory.entity;

import com.doosan.erp.common.entity.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

/**
 * 재고 엔티티
 *
 * 품목별, 창고별 재고 정보를 저장하는 핵심 엔티티입니다.
 * 재고 수량, 가용 수량, 예약 수량을 관리합니다.
 *
 * 테이블: stocks
 *
 * 주요 필드:
 * - itemCode/itemName: 품목 정보
 * - warehouseCode/warehouseName: 창고 정보
 * - quantity: 총 재고 수량
 * - availableQuantity: 가용 재고 수량 (총 수량 - 예약 수량)
 * - reservedQuantity: 예약된 재고 수량
 * - unitPrice: 단가
 *
 * BaseEntity를 상속하여 ID, 생성일시, 수정일시, Soft Delete 기능을 제공받습니다.
 */
@Entity
@Table(name = "stocks")
@Getter
@Setter
@NoArgsConstructor
public class Stock extends BaseEntity {

    // 품목 코드
    @Column(name = "item_code", nullable = false, length = 50)
    private String itemCode;

    // 품목명
    @Column(name = "item_name", nullable = false, length = 200)
    private String itemName;

    // 창고 코드
    @Column(name = "warehouse_code", nullable = false, length = 50)
    private String warehouseCode;

    // 창고명
    @Column(name = "warehouse_name", length = 200)
    private String warehouseName;

    // 총 재고 수량
    @Column(name = "quantity", precision = 19, scale = 2, nullable = false)
    private BigDecimal quantity = BigDecimal.ZERO;

    // 가용 재고 수량: 총 수량에서 예약 수량을 뺀 값
    @Column(name = "available_quantity", precision = 19, scale = 2, nullable = false)
    private BigDecimal availableQuantity = BigDecimal.ZERO;

    // 예약된 재고 수량: 수주 등으로 인해 예약된 수량
    @Column(name = "reserved_quantity", precision = 19, scale = 2, nullable = false)
    private BigDecimal reservedQuantity = BigDecimal.ZERO;

    // 단위 (예: 개, 박스, kg)
    @Column(name = "unit", nullable = false, length = 20)
    private String unit;

    // 단가
    @Column(name = "unit_price", precision = 19, scale = 2)
    private BigDecimal unitPrice;

    /**
     * 재고 생성자
     *
     * @param itemCode 품목 코드
     * @param itemName 품목명
     * @param warehouseCode 창고 코드
     * @param warehouseName 창고명
     * @param quantity 총 재고 수량
     * @param unit 단위
     * @param unitPrice 단가
     */
    public Stock(String itemCode, String itemName, String warehouseCode, String warehouseName,
                BigDecimal quantity, String unit, BigDecimal unitPrice) {
        this.itemCode = itemCode;
        this.itemName = itemName;
        this.warehouseCode = warehouseCode;
        this.warehouseName = warehouseName;
        this.quantity = quantity;
        this.availableQuantity = quantity;
        this.reservedQuantity = BigDecimal.ZERO;
        this.unit = unit;
        this.unitPrice = unitPrice;
    }

    /**
     * 재고 예약
     *
     * 수주 생성 시 가용 재고를 예약합니다.
     * 가용 재고에서 차감하고 예약 수량에 추가합니다.
     *
     * @param qty 예약할 수량
     * @throws IllegalStateException 가용 재고가 부족한 경우
     */
    public void reserve(BigDecimal qty) {
        if (this.availableQuantity.compareTo(qty) < 0) {
            throw new IllegalStateException("가용 재고가 부족합니다");
        }
        this.availableQuantity = this.availableQuantity.subtract(qty);
        this.reservedQuantity = this.reservedQuantity.add(qty);
    }

    /**
     * 재고 예약 해제
     *
     * 수주 취소 등으로 인해 예약된 재고를 해제합니다.
     * 예약 수량에서 차감하고 가용 재고에 추가합니다.
     *
     * @param qty 해제할 수량
     * @throws IllegalStateException 예약 재고가 부족한 경우
     */
    public void releaseReservation(BigDecimal qty) {
        if (this.reservedQuantity.compareTo(qty) < 0) {
            throw new IllegalStateException("예약 재고가 부족합니다");
        }
        this.reservedQuantity = this.reservedQuantity.subtract(qty);
        this.availableQuantity = this.availableQuantity.add(qty);
    }

    /**
     * 재고 차감 (실제 출고)
     *
     * 실제 출고 시 총 재고와 예약 재고를 차감합니다.
     * 예약된 재고만 출고할 수 있습니다.
     *
     * @param qty 차감할 수량
     * @throws IllegalStateException 예약 재고가 부족한 경우
     */
    public void deduct(BigDecimal qty) {
        if (this.reservedQuantity.compareTo(qty) < 0) {
            throw new IllegalStateException("예약 재고가 부족합니다");
        }
        this.quantity = this.quantity.subtract(qty);
        this.reservedQuantity = this.reservedQuantity.subtract(qty);
    }

    /**
     * 재고 증가 (입고)
     *
     * 입고 시 총 재고와 가용 재고를 증가시킵니다.
     *
     * @param qty 증가할 수량
     */
    public void increase(BigDecimal qty) {
        this.quantity = this.quantity.add(qty);
        this.availableQuantity = this.availableQuantity.add(qty);
    }
}
