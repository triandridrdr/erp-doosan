package com.doosan.erp.style.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class StyleRequest {

    private String productId;

    @NotBlank
    private String styleCode;

    @NotBlank
    private String styleName;

    private String season;

    private String description;

    private Long defaultBomMasterId;
}
