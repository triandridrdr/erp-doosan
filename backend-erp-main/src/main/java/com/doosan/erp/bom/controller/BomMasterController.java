package com.doosan.erp.bom.controller;

import com.doosan.erp.bom.dto.BomMasterCreateRequest;
import com.doosan.erp.bom.dto.BomMasterListItemResponse;
import com.doosan.erp.bom.dto.BomMasterResponse;
import com.doosan.erp.bom.service.BomMasterService;
import com.doosan.erp.common.dto.ApiResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/bom/masters")
@RequiredArgsConstructor
public class BomMasterController {

    private final BomMasterService bomMasterService;

    @GetMapping
    public ResponseEntity<ApiResponse<List<BomMasterListItemResponse>>> search(
            @RequestParam(name = "styleNo", required = false) String styleNo,
            @RequestParam(name = "article", required = false) String article
    ) {
        List<BomMasterListItemResponse> out = bomMasterService.search(styleNo, article)
                .stream()
                .map(BomMasterListItemResponse::from)
                .collect(Collectors.toList());
        return ResponseEntity.ok(ApiResponse.success(out));
    }

    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<BomMasterResponse>> get(@PathVariable("id") Long id) {
        return ResponseEntity.ok(ApiResponse.success(bomMasterService.get(id)));
    }

    @PostMapping
    public ResponseEntity<ApiResponse<BomMasterResponse>> create(@Valid @RequestBody BomMasterCreateRequest request) {
        BomMasterResponse res = bomMasterService.create(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(ApiResponse.success(res, "BoM master created"));
    }
}
