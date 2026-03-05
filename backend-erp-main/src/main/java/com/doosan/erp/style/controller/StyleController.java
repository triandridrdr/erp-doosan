package com.doosan.erp.style.controller;

import com.doosan.erp.bom.dto.BomMasterLineResponse;
import com.doosan.erp.common.dto.ApiResponse;
import com.doosan.erp.common.dto.PageResponse;
import com.doosan.erp.style.dto.StyleRequest;
import com.doosan.erp.style.dto.StyleResponse;
import com.doosan.erp.style.service.StyleService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/styles")
@RequiredArgsConstructor
@Tag(name = "Master - Style", description = "Style master data API")
public class StyleController {

    private final StyleService styleService;

    @PostMapping
    @Operation(summary = "Create style")
    public ResponseEntity<ApiResponse<StyleResponse>> create(@Valid @RequestBody StyleRequest request) {
        StyleResponse response = styleService.create(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(ApiResponse.success(response, "Style created"));
    }

    @GetMapping("/{id}")
    @Operation(summary = "Get style")
    public ResponseEntity<ApiResponse<StyleResponse>> getOne(@PathVariable("id") Long id) {
        return ResponseEntity.ok(ApiResponse.success(styleService.getOne(id)));
    }

    @GetMapping
    @Operation(summary = "List styles (search + pagination)")
    public ResponseEntity<ApiResponse<PageResponse<StyleResponse>>> list(
            @Parameter(description = "페이지 번호 (0부터 시작)") @RequestParam(name = "page", defaultValue = "0") int page,
            @Parameter(description = "페이지당 항목 수") @RequestParam(name = "size", defaultValue = "20") int size,
            @Parameter(description = "검색어 (styleCode/styleName/season)") @RequestParam(name = "search", required = false) String search
    ) {
        return ResponseEntity.ok(ApiResponse.success(styleService.search(search, page, size)));
    }

    @PutMapping("/{id}")
    @Operation(summary = "Update style")
    public ResponseEntity<ApiResponse<StyleResponse>> update(@PathVariable("id") Long id, @Valid @RequestBody StyleRequest request) {
        return ResponseEntity.ok(ApiResponse.success(styleService.update(id, request), "Style updated"));
    }

    @GetMapping("/{id}/default-bom-lines")
    @Operation(summary = "Get default BoM lines for style")
    public ResponseEntity<ApiResponse<List<BomMasterLineResponse>>> getDefaultBomLines(@PathVariable("id") Long id) {
        return ResponseEntity.ok(ApiResponse.success(styleService.getDefaultBomLines(id)));
    }

    @DeleteMapping("/{id}")
    @Operation(summary = "Delete style")
    public ResponseEntity<ApiResponse<Void>> delete(@PathVariable("id") Long id) {
        styleService.delete(id);
        return ResponseEntity.ok(ApiResponse.success(null, "Style deleted"));
    }
}
