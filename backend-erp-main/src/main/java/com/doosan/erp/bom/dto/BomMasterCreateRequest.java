package com.doosan.erp.bom.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class BomMasterCreateRequest {

    @NotBlank
    private String styleNo;

    @NotBlank
    private String article;

    @Valid
    @NotEmpty
    private List<BomMasterLineRequest> lines;
}
