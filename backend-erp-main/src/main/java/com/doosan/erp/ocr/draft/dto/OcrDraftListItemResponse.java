package com.doosan.erp.ocr.draft.dto;

import com.doosan.erp.ocr.draft.entity.OcrSalesOrderDraft;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
@AllArgsConstructor
public class OcrDraftListItemResponse {

    private Long id;
    private String sourceFilename;
    private String soNumber;
    private OcrSalesOrderDraft.DraftStatus status;
    private LocalDateTime createdAt;

    public static OcrDraftListItemResponse from(OcrSalesOrderDraft e) {
        return new OcrDraftListItemResponse(
                e.getId(),
                e.getSourceFilename(),
                e.getSoNumber(),
                e.getStatus(),
                e.getCreatedAt()
        );
    }
}
