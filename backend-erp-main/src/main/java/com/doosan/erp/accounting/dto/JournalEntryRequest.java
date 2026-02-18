package com.doosan.erp.accounting.dto;

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
 * 회계전표 생성/수정 요청 DTO
 *
 * 회계전표 생성 또는 수정 API 요청 시 클라이언트가 전송하는 데이터입니다.
 * Bean Validation을 통해 필수값 검증을 수행합니다.
 *
 * 전표 헤더 정보와 라인(분개 항목) 목록을 포함합니다.
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class JournalEntryRequest {

    // 전표일자 (필수)
    @NotNull(message = "전표일자는 필수입니다")
    private LocalDate entryDate;

    // 전표 설명 (선택)
    private String description;

    // 전표 라인 목록 (최소 1개 이상 필수)
    // @Valid: 중첩된 객체도 검증 수행
    @NotEmpty(message = "전표 라인은 최소 1개 이상 필요합니다")
    @Valid
    private List<JournalEntryLineRequest> lines;

    /**
     * 회계전표 라인 요청 DTO
     *
     * 개별 분개 항목에 대한 정보입니다.
     * 라인번호, 계정코드, 계정명, 차변/대변 금액은 필수 입력 항목입니다.
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class JournalEntryLineRequest {

        // 라인 번호 (필수): 전표 내 순서
        @NotNull(message = "라인번호는 필수입니다")
        private Integer lineNumber;

        // 계정과목 코드 (필수)
        @NotBlank(message = "계정코드는 필수입니다")
        private String accountCode;

        // 계정과목 명 (필수)
        @NotBlank(message = "계정명은 필수입니다")
        private String accountName;

        // 차변 금액 (필수)
        @NotNull(message = "차변금액은 필수입니다")
        private BigDecimal debit;

        // 대변 금액 (필수)
        @NotNull(message = "대변금액은 필수입니다")
        private BigDecimal credit;

        // 라인 설명 (선택)
        private String description;
    }
}
