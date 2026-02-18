package com.doosan.erp.accounting.dto;

import com.doosan.erp.accounting.entity.JournalEntry;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 회계전표 응답 DTO
 *
 * 회계전표 조회 API 응답 시 클라이언트에게 반환하는 데이터입니다.
 * 엔티티를 DTO로 변환하는 from() 정적 메서드를 제공합니다.
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class JournalEntryResponse {

    // 전표 ID
    private Long id;

    // 전표번호
    private String entryNumber;

    // 전표일자
    private LocalDate entryDate;

    // 전표 상태 (DRAFT, POSTED, CANCELLED)
    private String status;

    // 전표 설명
    private String description;

    // 차변 합계
    private BigDecimal totalDebit;

    // 대변 합계
    private BigDecimal totalCredit;

    // 전표 라인 목록
    private List<JournalEntryLineResponse> lines;

    // 생성일시
    private LocalDateTime createdAt;

    // 생성자
    private String createdBy;

    /**
     * 엔티티를 응답 DTO로 변환
     *
     * @param entity 변환할 전표 엔티티
     * @return 변환된 응답 DTO
     */
    public static JournalEntryResponse from(JournalEntry entity) {
        return new JournalEntryResponse(
                entity.getId(),
                entity.getEntryNumber(),
                entity.getEntryDate(),
                entity.getStatus().name(),
                entity.getDescription(),
                entity.getTotalDebit(),
                entity.getTotalCredit(),
                entity.getLines().stream()
                        .map(JournalEntryLineResponse::from)
                        .collect(Collectors.toList()),
                entity.getCreatedAt(),
                entity.getCreatedBy());
    }

    /**
     * 회계전표 라인 응답 DTO
     *
     * 전표 라인의 상세 정보를 담는 내부 클래스입니다.
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class JournalEntryLineResponse {
        // 라인 ID
        private Long id;

        // 라인 번호
        private Integer lineNumber;

        // 계정과목 코드
        private String accountCode;

        // 계정과목 명
        private String accountName;

        // 차변 금액
        private BigDecimal debit;

        // 대변 금액
        private BigDecimal credit;

        // 라인 설명
        private String description;

        /**
         * 엔티티를 응답 DTO로 변환
         *
         * @param entity 변환할 전표 라인 엔티티
         * @return 변환된 응답 DTO
         */
        public static JournalEntryLineResponse from(com.doosan.erp.accounting.entity.JournalEntryLine entity) {
            return new JournalEntryLineResponse(
                    entity.getId(),
                    entity.getLineNumber(),
                    entity.getAccountCode(),
                    entity.getAccountName(),
                    entity.getDebit(),
                    entity.getCredit(),
                    entity.getDescription());
        }
    }
}
