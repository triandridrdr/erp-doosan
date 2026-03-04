package com.doosan.erp.bom.dto;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class BomMasterLineRequest {

    @NotNull
    private Integer lineNo;

    private String component;

    private String category;

    private String composition;

    private String uom;

    private String consumptionPerUnit;

    private String wastePercent;
}
