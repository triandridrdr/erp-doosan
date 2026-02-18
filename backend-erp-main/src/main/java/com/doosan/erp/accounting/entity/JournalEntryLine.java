package com.doosan.erp.accounting.entity;

import com.doosan.erp.common.entity.BaseEntity;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

/**
 * 회계전표 라인 엔티티
 *
 * 회계전표의 개별 분개 항목을 나타내는 엔티티입니다.
 * 하나의 전표는 여러 개의 라인을 가질 수 있습니다.
 *
 * 테이블: journal_entry_lines
 *
 * 주요 필드:
 * - journalEntry: 소속 전표 (N:1 관계)
 * - lineNumber: 라인 번호 (전표 내 순서)
 * - accountCode/accountName: 계정과목 코드 및 명
 * - debit/credit: 차변/대변 금액
 *
 * BaseEntity를 상속하여 ID, 생성일시, 수정일시, Soft Delete 기능을 제공받습니다.
 */
@Entity
@Table(name = "journal_entry_lines")
@Getter
@Setter
@NoArgsConstructor
public class JournalEntryLine extends BaseEntity {

    // 소속 전표: N:1 관계, 지연 로딩
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "journal_entry_id", nullable = false)
    private JournalEntry journalEntry;

    // 라인 번호: 전표 내 순서
    @Column(name = "line_number", nullable = false)
    private Integer lineNumber;

    // 계정과목 코드
    @Column(name = "account_code", nullable = false, length = 50)
    private String accountCode;

    // 계정과목 명
    @Column(name = "account_name", nullable = false, length = 200)
    private String accountName;

    // 차변 금액
    @Column(name = "debit", precision = 19, scale = 2, nullable = false)
    private BigDecimal debit = BigDecimal.ZERO;

    // 대변 금액
    @Column(name = "credit", precision = 19, scale = 2, nullable = false)
    private BigDecimal credit = BigDecimal.ZERO;

    // 라인 설명
    @Column(name = "description", length = 500)
    private String description;

    /**
     * 전표 라인 생성자
     *
     * @param lineNumber 라인 번호
     * @param accountCode 계정과목 코드
     * @param accountName 계정과목 명
     * @param debit 차변 금액
     * @param credit 대변 금액
     * @param description 라인 설명
     */
    public JournalEntryLine(Integer lineNumber, String accountCode, String accountName,
                           BigDecimal debit, BigDecimal credit, String description) {
        this.lineNumber = lineNumber;
        this.accountCode = accountCode;
        this.accountName = accountName;
        this.debit = debit;
        this.credit = credit;
        this.description = description;
    }
}
