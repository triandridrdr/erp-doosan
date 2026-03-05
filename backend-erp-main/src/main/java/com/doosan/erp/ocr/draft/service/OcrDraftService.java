package com.doosan.erp.ocr.draft.service;

import com.doosan.erp.bom.entity.BomMaster;
import com.doosan.erp.bom.entity.BomMasterLine;
import com.doosan.erp.bom.repository.BomMasterLineRepository;
import com.doosan.erp.bom.repository.BomMasterRepository;
import com.doosan.erp.ocr.draft.dto.OcrDraftSaveRequest;
import com.doosan.erp.ocr.draft.dto.OcrDraftUpdateRequest;
import com.doosan.erp.ocr.draft.entity.OcrSalesOrderDraft;
import com.doosan.erp.ocr.draft.repository.OcrSalesOrderDraftRepository;
import com.doosan.erp.common.constant.ErrorCode;
import com.doosan.erp.common.exception.ResourceNotFoundException;
import com.doosan.erp.style.entity.Style;
import com.doosan.erp.style.repository.StyleRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class OcrDraftService {

    private final OcrSalesOrderDraftRepository ocrSalesOrderDraftRepository;
    private final ObjectMapper objectMapper;
    private final BomMasterRepository bomMasterRepository;
    private final BomMasterLineRepository bomMasterLineRepository;
    private final StyleRepository styleRepository;

    private static Long readLong(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) return null;
        if (node.isNumber()) return node.longValue();
        if (node.isTextual()) {
            String s = node.asText("").trim();
            if (s.isEmpty()) return null;
            try {
                return Long.parseLong(s);
            } catch (Exception ignored) {
                return null;
            }
        }
        return null;
    }

    private static String readText(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) return "";
        if (node.isTextual()) return node.asText("");
        return node.toString();
    }

    private static String findHeaderValue(JsonNode draft, String fieldName) {
        if (draft == null || !draft.isObject()) return "";
        JsonNode headerRows = draft.get("headerRows");
        if (headerRows == null || !headerRows.isArray()) return "";
        for (JsonNode row : headerRows) {
            if (row == null || !row.isObject()) continue;
            String field = readText(row.get("field")).trim();
            if (field.equalsIgnoreCase(fieldName)) {
                return readText(row.get("value")).trim();
            }
        }
        return "";
    }

    private String ensureBomMasterLinked(String draftJson) {
        try {
            JsonNode root = objectMapper.readTree(draftJson);
            if (root == null || !root.isObject()) return draftJson;
            ObjectNode draft = (ObjectNode) root;

            ObjectNode system;
            JsonNode systemNode = draft.get("system");
            if (systemNode != null && systemNode.isObject()) {
                system = (ObjectNode) systemNode;
            } else {
                system = objectMapper.createObjectNode();
                draft.set("system", system);
            }

            Long existingBomMasterId = readLong(system.get("bomMasterId"));
            Long styleId = readLong(system.get("styleId"));
            Style style = null;
            if (styleId != null) {
                style = styleRepository.findById(styleId).orElse(null);
            }

            if (existingBomMasterId != null) {
                return objectMapper.writeValueAsString(draft);
            }

            String styleNo = style != null ? (style.getStyleCode() == null ? "" : style.getStyleCode().trim()) : "";
            if (styleNo.isEmpty()) styleNo = "OCR";

            String article = findHeaderValue(draft, "Article");
            if (article.isEmpty()) article = findHeaderValue(draft, "Style");
            if (article.isEmpty()) article = style != null ? (style.getStyleName() == null ? "" : style.getStyleName().trim()) : "";
            if (article.isEmpty()) article = "OCR";

            JsonNode bomRowsNode = draft.get("bomRows");
            if (bomRowsNode == null || !bomRowsNode.isArray() || bomRowsNode.size() == 0) {
                return objectMapper.writeValueAsString(draft);
            }

            BomMaster m = new BomMaster();
            m.setStyleNo(styleNo);
            m.setArticle(article);
            m.setRevision(1);
            m.setStatus(BomMaster.BomStatus.ACTIVE);
            BomMaster saved = bomMasterRepository.save(m);

            int lineNo = 1;
            for (JsonNode row : (ArrayNode) bomRowsNode) {
                if (row == null || !row.isObject()) continue;
                BomMasterLine line = new BomMasterLine();
                line.setBomMaster(saved);
                line.setLineNo(lineNo++);
                line.setComponent(readText(row.get("component")));
                line.setCategory(readText(row.get("category")));
                line.setComposition(readText(row.get("composition")));
                line.setUom(readText(row.get("uom")));
                line.setConsumptionPerUnit(readText(row.get("consumptionPerUnit")));
                line.setWastePercent(readText(row.get("wastePercent")));
                bomMasterLineRepository.save(line);
            }

            system.put("bomMasterId", saved.getId());

            return objectMapper.writeValueAsString(draft);
        } catch (Exception ignored) {
            return draftJson;
        }
    }

    public Long save(OcrDraftSaveRequest request) {
        OcrSalesOrderDraft e = new OcrSalesOrderDraft();
        e.setSourceFilename(request.getSourceFilename());
        e.setSoNumber(request.getSoNumber());
        e.setDraftJson(ensureBomMasterLinked(request.getDraft().toString()));
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
        e.setDraftJson(ensureBomMasterLinked(request.getDraft().toString()));
        return e;
    }

    @Transactional
    public OcrSalesOrderDraft approve(long id) {
        OcrSalesOrderDraft e = getById(id);

        e.setDraftJson(ensureBomMasterLinked(e.getDraftJson()));

        e.setStatus(OcrSalesOrderDraft.DraftStatus.APPROVED);
        return e;
    }

    @Transactional
    public void delete(long id) {
        OcrSalesOrderDraft e = getById(id);
        e.setStatus(OcrSalesOrderDraft.DraftStatus.DELETED);
    }
}
