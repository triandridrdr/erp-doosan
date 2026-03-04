package com.doosan.erp.ocr.draft.dto;

import com.doosan.erp.ocr.draft.entity.OcrSalesOrderDraft;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
@AllArgsConstructor
public class OcrDraftResponse {

    private Long id;
    private String sourceFilename;
    private String soNumber;
    private OcrSalesOrderDraft.DraftStatus status;
    private LocalDateTime createdAt;
    private JsonNode draft;

    public static OcrDraftResponse from(OcrSalesOrderDraft e, ObjectMapper objectMapper) {
        JsonNode node;
        try {
            node = objectMapper.readTree(e.getDraftJson());
        } catch (Exception ex) {
            node = null;
        }

        return new OcrDraftResponse(
                e.getId(),
                e.getSourceFilename(),
                e.getSoNumber(),
                e.getStatus(),
                e.getCreatedAt(),
                node
        );
    }
}
