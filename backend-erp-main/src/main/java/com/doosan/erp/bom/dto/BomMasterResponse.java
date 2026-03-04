package com.doosan.erp.bom.dto;

import com.doosan.erp.bom.entity.BomMaster;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.LocalDateTime;
import java.util.List;

@Getter
@AllArgsConstructor
public class BomMasterResponse {

    private Long id;
    private String styleNo;
    private String article;
    private Integer revision;
    private BomMaster.BomStatus status;
    private LocalDateTime createdAt;
    private List<BomMasterLineResponse> lines;
}
