package com.doosan.erp.ocr.draft.controller;

import com.doosan.erp.common.dto.ApiResponse;
import com.doosan.erp.ocr.draft.dto.OcrDraftSaveRequest;
import com.doosan.erp.ocr.draft.dto.OcrDraftSaveResponse;
import com.doosan.erp.ocr.draft.service.OcrDraftService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/ocr/drafts")
@RequiredArgsConstructor
public class OcrDraftController {

    private final OcrDraftService ocrDraftService;

    @PostMapping
    public ResponseEntity<ApiResponse<OcrDraftSaveResponse>> save(@Valid @RequestBody OcrDraftSaveRequest request) {
        Long id = ocrDraftService.save(request);
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(ApiResponse.success(new OcrDraftSaveResponse(id), "OCR draft saved"));
    }
}
