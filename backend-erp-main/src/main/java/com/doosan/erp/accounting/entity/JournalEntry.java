package com.doosan.erp.accounting.entity;

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
 * 회계전표 엔티티
 *
 * 회계 분개장의 전표 정보를 저장하는 핵심 엔티티입니다.
 * 전표 헤더(이 클래스)와 전표 라인(JournalEntryLine)으로 구성됩니다.
 *
 * 테이블: journal_entries
 *
 * 주요 필드:
 * - entryNumber: 전표번호 (고유값, 예: JE-2024-1001)
 * - entryDate: 전표일자
 * - status: 전표 상태 (작성중/전기완료/취소)
 * - totalDebit/totalCredit: 차변/대변 합계 (자동 계산)
 * - lines: 전표 라인 목록
 *
 * BaseEntity를 상속하여 ID, 생성일시, 수정일시, Soft Delete 기능을 제공받습니다.
 */
@Entity
@Table(name = "journal_entries")
@Getter
@Setter
@NoArgsConstructor
public class JournalEntry extends BaseEntity {

    // 전표번호: 시스템에서 자동 생성되는 고유 식별자
    @Column(name = "entry_number", unique = true, nullable = false, length = 50)
    private String entryNumber;

    // 전표일자
    @Column(name = "entry_date", nullable = false)
    private LocalDate entryDate;

    // 전표 상태: 기본값은 작성중(DRAFT)
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private EntryStatus status = EntryStatus.DRAFT;

    // 전표 설명
    @Column(name = "description", length = 500)
    private String description;

    // 차변 합계: 모든 라인의 차변 금액 합계 (자동 계산)
    @Column(name = "total_debit", precision = 19, scale = 2, nullable = false)
    private BigDecimal totalDebit = BigDecimal.ZERO;

    // 대변 합계: 모든 라인의 대변 금액 합계 (자동 계산)
    @Column(name = "total_credit", precision = 19, scale = 2, nullable = false)
    private BigDecimal totalCredit = BigDecimal.ZERO;

    // 전표 라인 목록: 1:N 관계, 전표 삭제 시 라인도 함께 삭제(cascade)
    // orphanRemoval: 라인이 목록에서 제거되면 DB에서도 삭제
    @OneToMany(mappedBy = "journalEntry", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    private List<JournalEntryLine> lines = new ArrayList<>();

    /**
     * 전표 상태 열거형
     *
     * DRAFT: 작성중 (수정 가능)
     * POSTED: 전기완료 (수정 불가)
     * CANCELLED: 취소
     */
    public enum EntryStatus {
        DRAFT, // 작성중
        POSTED, // 전기완료
        CANCELLED // 취소
    }

    /**
     * 전표 라인 추가
     *
     * 라인을 추가하고 양방향 관계를 설정한 후 차변/대변 합계를 재계산합니다.
     * 양방향 관계 설정을 위해 line.setJournalEntry(this)를 호출합니다.
     *
     * @param line 추가할 전표 라인
     */
    public void addLine(JournalEntryLine line) {
        lines.add(line);
        line.setJournalEntry(this);
        recalculateTotals();
    }

    /**
     * 전표 라인 제거
     *
     * 라인을 제거하고 양방향 관계를 해제한 후 차변/대변 합계를 재계산합니다.
     *
     * @param line 제거할 전표 라인
     */
    public void removeLine(JournalEntryLine line) {
        lines.remove(line);
        line.setJournalEntry(null);
        recalculateTotals();
    }

    /**
     * 차변/대변 합계 재계산
     *
     * 모든 라인의 차변/대변 금액을 각각 합산하여 총합을 갱신합니다.
     * 라인이 추가/수정/삭제될 때마다 호출됩니다.
     */
    public void recalculateTotals() {
        this.totalDebit = lines.stream()
                .map(JournalEntryLine::getDebit)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        this.totalCredit = lines.stream()
                .map(JournalEntryLine::getCredit)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    /**
     * 전표 전기
     *
     * 작성중 상태의 전표를 전기완료 상태로 변경합니다.
     * 이미 전기되었거나 차변과 대변이 일치하지 않으면 전기할 수 없습니다.
     *
     * @throws IllegalStateException 이미 전기되었거나 차대변 불일치인 경우
     */
    public void post() {
        if (this.status == EntryStatus.POSTED) {
            throw new IllegalStateException("이미 전기된 전표입니다");
        }
        if (!isBalanced()) {
            throw new IllegalStateException("차변과 대변이 일치하지 않습니다");
        }
        this.status = EntryStatus.POSTED;
    }

    /**
     * 차대변 균형 확인
     *
     * 차변 합계와 대변 합계가 일치하는지 확인합니다.
     * 회계 원칙상 차변과 대변은 항상 일치해야 합니다.
     *
     * @return 차변과 대변이 일치하면 true, 아니면 false
     */
    public boolean isBalanced() {
        return totalDebit.compareTo(totalCredit) == 0;
    }
}
