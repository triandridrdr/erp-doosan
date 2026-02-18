package com.doosan.erp.sales.entity;

import com.doosan.erp.common.entity.BaseEntity;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * 수주 엔티티
 *
 * 고객으로부터 받은 주문 정보를 저장하는 핵심 엔티티입니다.
 * 수주 헤더(이 클래스)와 수주 라인(SalesOrderLine)으로 구성됩니다.
 *
 * 테이블: sales_orders
 *
 * 주요 필드:
 * - orderNumber: 수주번호 (고유값, 예: SO-2024-1001)
 * - orderDate: 수주일자
 * - customerCode, customerName: 고객 정보
 * - status: 수주 상태 (대기/확정/출하완료/취소)
 * - totalAmount: 총 금액 (라인 금액의 합계, 자동 계산)
 * - lines: 수주 라인 목록 (주문 상품 목록)
 *
 * BaseEntity를 상속하여 ID, 생성일시, 수정일시, Soft Delete 기능을 제공받습니다.
 */
@Entity
@Table(name = "sales_orders")
@Getter
@Setter
@NoArgsConstructor
public class SalesOrder extends BaseEntity {

    // 수주번호: 시스템에서 자동 생성되는 고유 식별자
    @Column(name = "order_number", unique = true, nullable = false, length = 50)
    private String orderNumber;

    // 수주일자
    @Column(name = "order_date", nullable = false)
    private LocalDate orderDate;

    // 고객코드: 고객 마스터의 코드 참조
    @Column(name = "customer_code", nullable = false, length = 50)
    private String customerCode;

    // 고객명
    @Column(name = "customer_name", nullable = false, length = 200)
    private String customerName;

    // 수주 상태: 기본값은 대기(PENDING)
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private OrderStatus status = OrderStatus.PENDING;

    // 총 금액: 모든 라인 금액의 합계 (자동 계산)
    @Column(name = "total_amount", precision = 19, scale = 2, nullable = false)
    private BigDecimal totalAmount = BigDecimal.ZERO;

    // 배송지 주소
    @Column(name = "delivery_address", length = 500)
    private String deliveryAddress;

    // 비고
    @Column(name = "remarks", length = 1000)
    private String remarks;

    // 수주 라인 목록: 1:N 관계, 수주 삭제 시 라인도 함께 삭제(cascade)
    // orphanRemoval: 라인이 목록에서 제거되면 DB에서도 삭제
    @OneToMany(mappedBy = "salesOrder", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    private List<SalesOrderLine> lines = new ArrayList<>();

    /**
     * 수주 상태 열거형
     *
     * PENDING: 대기 (수정 가능)
     * CONFIRMED: 확정 (수정 불가)
     * SHIPPED: 출하완료 (삭제 불가)
     * CANCELLED: 취소
     */
    public enum OrderStatus {
        PENDING,    // 대기
        CONFIRMED,  // 확정
        SHIPPED,    // 출하완료
        CANCELLED   // 취소
    }

    /**
     * 수주 라인 추가
     *
     * 라인을 추가하고 양방향 관계를 설정한 후 총 금액을 재계산합니다.
     * 양방향 관계 설정을 위해 line.setSalesOrder(this)를 호출합니다.
     *
     * @param line 추가할 수주 라인
     */
    public void addLine(SalesOrderLine line) {
        lines.add(line);
        line.setSalesOrder(this);
        recalculateTotalAmount();
    }

    /**
     * 수주 라인 제거
     *
     * 라인을 제거하고 양방향 관계를 해제한 후 총 금액을 재계산합니다.
     *
     * @param line 제거할 수주 라인
     */
    public void removeLine(SalesOrderLine line) {
        lines.remove(line);
        line.setSalesOrder(null);
        recalculateTotalAmount();
    }

    /**
     * 총 금액 재계산
     *
     * 모든 라인의 금액(수량 * 단가)을 합산하여 총 금액을 갱신합니다.
     * 라인이 추가/수정/삭제될 때마다 호출됩니다.
     */
    public void recalculateTotalAmount() {
        this.totalAmount = lines.stream()
                .map(SalesOrderLine::getLineAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    /**
     * 수주 확정
     *
     * 대기 상태의 수주를 확정 상태로 변경합니다.
     * 이미 확정되었거나 취소된 수주는 확정할 수 없습니다.
     *
     * @throws IllegalStateException 이미 확정되었거나 취소된 경우
     */
    public void confirm() {
        if (this.status == OrderStatus.CONFIRMED) {
            throw new IllegalStateException("이미 확정된 수주입니다");
        }
        if (this.status == OrderStatus.CANCELLED) {
            throw new IllegalStateException("취소된 수주는 확정할 수 없습니다");
        }
        this.status = OrderStatus.CONFIRMED;
    }

    /**
     * 수주 취소
     *
     * 수주를 취소 상태로 변경합니다.
     * 이미 출하완료된 수주는 취소할 수 없습니다.
     *
     * @throws IllegalStateException 출하완료된 수주인 경우
     */
    public void cancel() {
        if (this.status == OrderStatus.SHIPPED) {
            throw new IllegalStateException("출하완료된 수주는 취소할 수 없습니다");
        }
        this.status = OrderStatus.CANCELLED;
    }
}
