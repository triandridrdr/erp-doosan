package com.doosan.erp.ocr.draft.service;

import com.doosan.erp.ocr.draft.dto.OcrDraftSaveRequest;
import com.doosan.erp.ocr.draft.dto.OcrDraftUpdateRequest;
import com.doosan.erp.ocr.draft.entity.OcrSalesOrderDraft;
import com.doosan.erp.ocr.draft.repository.OcrSalesOrderDraftRepository;
import com.doosan.erp.common.constant.ErrorCode;
import com.doosan.erp.common.exception.ResourceNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class OcrDraftService {

    private final OcrSalesOrderDraftRepository ocrSalesOrderDraftRepository;

    public Long save(OcrDraftSaveRequest request) {
        OcrSalesOrderDraft e = new OcrSalesOrderDraft();
        e.setSourceFilename(request.getSourceFilename());
        e.setSoNumber(request.getSoNumber());
        e.setDraftJson(request.getDraft().toString());
        e.setStatus(OcrSalesOrderDraft.DraftStatus.DRAFT);
        return ocrSalesOrderDraftRepository.save(e).getId();
    }

    public List<OcrSalesOrderDraft> listAll() {
        return ocrSalesOrderDraftRepository.findAll();
    }

    public OcrSalesOrderDraft getById(long id) {
        return ocrSalesOrderDraftRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.RESOURCE_NOT_FOUND, "OCR draft not found"));
    }

    @Transactional
    public OcrSalesOrderDraft update(long id, OcrDraftUpdateRequest request) {
        OcrSalesOrderDraft e = getById(id);
        if (request.getSourceFilename() != null) e.setSourceFilename(request.getSourceFilename());
        if (request.getSoNumber() != null) e.setSoNumber(request.getSoNumber());
        e.setDraftJson(request.getDraft().toString());
        return e;
    }

    @Transactional
    public OcrSalesOrderDraft approve(long id) {
        OcrSalesOrderDraft e = getById(id);
        e.setStatus(OcrSalesOrderDraft.DraftStatus.APPROVED);
        return e;
    }

    @Transactional
    public void delete(long id) {
        OcrSalesOrderDraft e = getById(id);
        ocrSalesOrderDraftRepository.delete(e);
    }
}
