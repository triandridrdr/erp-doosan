package com.doosan.erp.accounting.controller;

import com.doosan.erp.accounting.dto.JournalEntryRequest;
import com.doosan.erp.accounting.dto.JournalEntryResponse;
import com.doosan.erp.accounting.service.JournalEntryService;
import com.doosan.erp.common.dto.ApiResponse;
import com.doosan.erp.common.dto.PageResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 회계전표 API 컨트롤러
 *
 * 회계전표(Journal Entry) 관련 REST API를 제공하는 컨트롤러입니다.
 * 회계전표는 회계 분개장의 기본 단위로, 차변과 대변이 일치하는 분개 항목들의 집합입니다.
 *
 * 제공 기능:
 * - 전표 생성 (POST /api/v1/accounting/journal-entries)
 * - 전표 조회 (GET /api/v1/accounting/journal-entries/{id})
 * - 전표 목록 조회 (GET /api/v1/accounting/journal-entries)
 * - 전표 전기 (POST /api/v1/accounting/journal-entries/{id}/post)
 * - 전표 삭제 (DELETE /api/v1/accounting/journal-entries/{id})
 */
@RestController
@RequestMapping("/api/v1/accounting/journal-entries")
@RequiredArgsConstructor
@Tag(name = "Accounting - Journal Entry", description = "회계전표 관리 API")
public class JournalEntryController {

    // 회계전표 비즈니스 로직을 처리하는 서비스
    private final JournalEntryService journalEntryService;

    /**
     * 회계전표 생성 API
     *
     * 새로운 회계전표를 등록합니다.
     * 전표일자, 설명, 그리고 최소 1개 이상의 분개 라인을 포함해야 합니다.
     * 차변과 대변의 합계가 일치해야 합니다 (회계 원칙).
     * 생성 성공 시 HTTP 201 상태코드와 함께 전표 정보를 반환합니다.
     *
     * @param request 전표 생성 요청 정보 (전표일자, 설명, 분개 라인 목록)
     * @return 생성된 전표 정보
     */
    @PostMapping
    @Operation(summary = "회계전표 생성", description = "새로운 회계전표를 생성합니다")
    public ResponseEntity<ApiResponse<JournalEntryResponse>> createJournalEntry(
            @Valid @RequestBody JournalEntryRequest request) {
        JournalEntryResponse response = journalEntryService.createJournalEntry(request);
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(ApiResponse.success(response, "회계전표가 생성되었습니다"));
    }

    /**
     * 회계전표 상세 조회 API
     *
     * 전표 ID로 해당 전표의 상세 정보를 조회합니다.
     * 전표 헤더 정보와 함께 모든 분개 라인 정보도 포함됩니다.
     *
     * @param id 전표 ID
     * @return 전표 상세 정보
     */
    @GetMapping("/{id}")
    @Operation(summary = "회계전표 조회", description = "회계전표 상세 정보를 조회합니다")
    public ResponseEntity<ApiResponse<JournalEntryResponse>> getJournalEntry(@PathVariable Long id) {
        JournalEntryResponse response = journalEntryService.getJournalEntry(id);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    /**
     * 회계전표 목록 조회 API
     *
     * 페이징 처리된 전표 목록을 조회합니다.
     * page와 size 파라미터로 페이지네이션을 제어할 수 있습니다.
     * 최신 생성순으로 정렬됩니다.
     *
     * @param page 페이지 번호 (0부터 시작, 기본값: 0)
     * @param size 페이지당 항목 수 (기본값: 20)
     * @return 페이징된 전표 목록
     */
    @GetMapping
    @Operation(summary = "회계전표 목록 조회", description = "회계전표 목록을 페이징하여 조회합니다")
    public ResponseEntity<ApiResponse<PageResponse<JournalEntryResponse>>> getJournalEntries(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        PageResponse<JournalEntryResponse> response = journalEntryService.getJournalEntries(page, size);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    /**
     * 회계전표 전기 API
     *
     * 작성중(DRAFT) 상태의 전표를 전기완료(POSTED) 상태로 변경합니다.
     * 전기 시 차대변 균형을 재검증하고, 전기 이벤트를 발행합니다.
     * 전기된 전표는 더 이상 수정할 수 없습니다.
     *
     * @param id 전표 ID
     * @return 전기된 전표 정보
     */
    @PostMapping("/{id}/post")
    @Operation(summary = "회계전표 전기", description = "회계전표를 전기 처리합니다")
    public ResponseEntity<ApiResponse<JournalEntryResponse>> postJournalEntry(@PathVariable Long id) {
        JournalEntryResponse response = journalEntryService.postJournalEntry(id);
        return ResponseEntity.ok(ApiResponse.success(response, "회계전표가 전기되었습니다"));
    }

    /**
     * 회계전표 삭제 API
     *
     * 전표를 삭제합니다. Soft Delete 방식으로 실제 데이터는 유지됩니다.
     * 전기된 전표는 삭제할 수 없으니 주의하세요.
     *
     * @param id 전표 ID
     * @return 삭제 성공 응답
     */
    @DeleteMapping("/{id}")
    @Operation(summary = "회계전표 삭제", description = "회계전표를 삭제합니다 (Soft Delete)")
    public ResponseEntity<ApiResponse<Void>> deleteJournalEntry(@PathVariable Long id) {
        journalEntryService.deleteJournalEntry(id);
        return ResponseEntity.ok(ApiResponse.success(null, "회계전표가 삭제되었습니다"));
    }
}
