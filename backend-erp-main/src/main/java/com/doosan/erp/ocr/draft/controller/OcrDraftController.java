package com.doosan.erp.ocr.draft.controller;

import com.doosan.erp.common.dto.ApiResponse;
import com.doosan.erp.ocr.draft.dto.OcrDraftApproveResponse;
import com.doosan.erp.ocr.draft.dto.OcrDraftListItemResponse;
import com.doosan.erp.ocr.draft.dto.OcrDraftResponse;
import com.doosan.erp.ocr.draft.dto.OcrDraftSaveRequest;
import com.doosan.erp.ocr.draft.dto.OcrDraftSaveResponse;
import com.doosan.erp.ocr.draft.dto.OcrDraftUpdateRequest;
import com.doosan.erp.ocr.draft.entity.OcrSalesOrderDraft;
import com.doosan.erp.ocr.draft.service.OcrDraftService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/ocr/drafts")
@RequiredArgsConstructor
public class OcrDraftController {

    private final OcrDraftService ocrDraftService;
    private final ObjectMapper objectMapper;

    @PostMapping
    public ResponseEntity<ApiResponse<OcrDraftSaveResponse>> save(@Valid @RequestBody OcrDraftSaveRequest request) {
        Long id = ocrDraftService.save(request);
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(ApiResponse.success(new OcrDraftSaveResponse(id), "OCR draft saved"));
    }

    @GetMapping
    public ResponseEntity<ApiResponse<List<OcrDraftListItemResponse>>> list() {
        List<OcrDraftListItemResponse> out = ocrDraftService.listAll().stream()
                .sorted(Comparator.comparing(OcrSalesOrderDraft::getCreatedAt, Comparator.nullsLast(Comparator.naturalOrder())).reversed())
                .map(OcrDraftListItemResponse::from)
                .collect(Collectors.toList());
        return ResponseEntity.ok(ApiResponse.success(out));
    }

    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<OcrDraftResponse>> get(@PathVariable("id") long id) {
        OcrSalesOrderDraft e = ocrDraftService.getById(id);
        return ResponseEntity.ok(ApiResponse.success(OcrDraftResponse.from(e, objectMapper)));
    }

    @PutMapping("/{id}")
    public ResponseEntity<ApiResponse<OcrDraftResponse>> update(@PathVariable("id") long id, @Valid @RequestBody OcrDraftUpdateRequest request) {
        OcrSalesOrderDraft e = ocrDraftService.update(id, request);
        return ResponseEntity.ok(ApiResponse.success(OcrDraftResponse.from(e, objectMapper), "OCR draft updated"));
    }

    @PostMapping("/{id}/approve")
    public ResponseEntity<ApiResponse<OcrDraftApproveResponse>> approve(@PathVariable("id") long id) {
        OcrSalesOrderDraft e = ocrDraftService.approve(id);
        return ResponseEntity.ok(ApiResponse.success(new OcrDraftApproveResponse(e.getId(), e.getStatus()), "OCR draft approved"));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<ApiResponse<Void>> delete(@PathVariable("id") long id) {
        ocrDraftService.delete(id);
        return ResponseEntity.ok(ApiResponse.success(null, "OCR draft deleted"));
    }
}
