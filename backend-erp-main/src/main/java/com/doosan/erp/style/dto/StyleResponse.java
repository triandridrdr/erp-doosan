package com.doosan.erp.style.dto;

import com.doosan.erp.style.entity.Style;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class StyleResponse {

    private Long id;
    private String productId;
    private String styleCode;
    private String styleName;
    private String season;
    private String description;
    private Long defaultBomMasterId;

    public static StyleResponse from(Style style) {
        return new StyleResponse(
                style.getId(),
                style.getProductId(),
                style.getStyleCode(),
                style.getStyleName(),
                style.getSeason(),
                style.getDescription(),
                style.getDefaultBomMasterId()
        );
    }
}
