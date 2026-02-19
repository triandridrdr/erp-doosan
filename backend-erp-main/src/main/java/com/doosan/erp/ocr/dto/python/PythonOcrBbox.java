package com.doosan.erp.ocr.dto.python;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PythonOcrBbox {

    private Integer left;
    private Integer top;
    private Integer width;
    private Integer height;
}
