package com.doosan.erp.ocr.draft.dto;

import com.doosan.erp.ocr.draft.entity.OcrSalesOrderDraft;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class OcrDraftApproveResponse {

    private Long id;
    private OcrSalesOrderDraft.DraftStatus status;
}
