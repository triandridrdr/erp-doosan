package com.doosan.erp.ocr.draft.service;

import com.doosan.erp.ocr.draft.dto.OcrDraftSaveRequest;
import com.doosan.erp.ocr.draft.entity.OcrSalesOrderDraft;
import com.doosan.erp.ocr.draft.repository.OcrSalesOrderDraftRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OcrDraftService {

    private final OcrSalesOrderDraftRepository ocrSalesOrderDraftRepository;

    public Long save(OcrDraftSaveRequest request) {
        OcrSalesOrderDraft e = new OcrSalesOrderDraft();
        e.setSourceFilename(request.getSourceFilename());
        e.setSoNumber(request.getSoNumber());
        e.setDraftJson(request.getDraft().toString());
        return ocrSalesOrderDraftRepository.save(e).getId();
    }
}
