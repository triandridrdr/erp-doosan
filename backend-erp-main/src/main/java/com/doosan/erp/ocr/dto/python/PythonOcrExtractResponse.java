package com.doosan.erp.ocr.dto.python;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PythonOcrExtractResponse {

    private String filename;
    private String engine;
    private String text;
    private List<PythonOcrPageResult> pages;
}
