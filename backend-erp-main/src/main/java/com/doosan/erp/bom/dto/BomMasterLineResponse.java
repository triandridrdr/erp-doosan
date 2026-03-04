package com.doosan.erp.bom.dto;

import com.doosan.erp.bom.entity.BomMasterLine;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class BomMasterLineResponse {

    private Integer lineNo;
    private String component;
    private String category;
    private String composition;
    private String uom;
    private String consumptionPerUnit;
    private String wastePercent;

    public static BomMasterLineResponse from(BomMasterLine e) {
        return new BomMasterLineResponse(
                e.getLineNo(),
                e.getComponent(),
                e.getCategory(),
                e.getComposition(),
                e.getUom(),
                e.getConsumptionPerUnit(),
                e.getWastePercent()
        );
    }
}
