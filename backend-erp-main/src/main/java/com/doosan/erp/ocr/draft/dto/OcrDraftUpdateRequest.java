package com.doosan.erp.ocr.draft.dto;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class OcrDraftUpdateRequest {

    private String sourceFilename;

    private String soNumber;

    @NotNull
    private JsonNode draft;
}
