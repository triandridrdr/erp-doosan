package com.doosan.erp.ocr.draft.service;

import com.doosan.erp.ocr.draft.dto.OcrDraftSaveRequest;
import com.doosan.erp.ocr.draft.dto.OcrDraftUpdateRequest;
import com.doosan.erp.ocr.draft.entity.OcrSalesOrderDraft;
import com.doosan.erp.ocr.draft.repository.OcrSalesOrderDraftRepository;
import com.doosan.erp.common.constant.ErrorCode;
import com.doosan.erp.common.exception.BusinessException;
import com.doosan.erp.common.exception.ResourceNotFoundException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class OcrDraftService {

    private final OcrSalesOrderDraftRepository ocrSalesOrderDraftRepository;
    private final ObjectMapper objectMapper;

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

        JsonNode root;
        try {
            root = objectMapper.readTree(e.getDraftJson());
        } catch (Exception ex) {
            throw new BusinessException(ErrorCode.INVALID_INPUT_VALUE, "Draft JSON is invalid");
        }

        JsonNode bomMasterId = root.path("system").path("bomMasterId");
        boolean hasBomMasterId = !bomMasterId.isMissingNode()
                && !bomMasterId.isNull()
                && !(bomMasterId.isTextual() && bomMasterId.asText().trim().isEmpty());

        if (!hasBomMasterId) {
            throw new BusinessException(ErrorCode.INVALID_INPUT_VALUE, "BoM master must be attached before approving");
        }

        e.setStatus(OcrSalesOrderDraft.DraftStatus.APPROVED);
        return e;
    }

    @Transactional
    public void delete(long id) {
        getById(id);
        ocrSalesOrderDraftRepository.deleteById(id);
    }
}
