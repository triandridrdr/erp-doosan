package com.doosan.erp.ocr.controller;

import com.doosan.erp.common.dto.ApiResponse;
import com.doosan.erp.ocr.dto.DocumentAnalysisResponse;
import com.doosan.erp.ocr.dto.OcrResponse;
import com.doosan.erp.ocr.service.OcrService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

/**
 * OCR API 컨트롤러
 *
 * Amazon Textract를 활용한 OCR(광학 문자 인식) REST API를 제공하는 컨트롤러입니다.
 * 이미지나 PDF 문서에서 텍스트를 추출하고, 테이블/폼 데이터를 구조화된 형태로 반환합니다.
 *
 * 제공 기능:
 * - 텍스트 추출 (POST /api/v1/ocr/extract) - 단순 텍스트 라인 추출
 * - 문서 분석 (POST /api/v1/ocr/analyze) - 테이블, 폼 필드 구조화 추출
 *
 * 지원 파일 형식:
 * - PNG, JPG, JPEG, PDF (최대 10MB)
 */
@Tag(name = "OCR", description = "OCR 텍스트 추출 API")
@RestController
@RequestMapping("/api/v1/ocr")
@RequiredArgsConstructor
public class OcrController {

    // OCR 비즈니스 로직을 처리하는 서비스
    private final OcrService ocrService;

    /**
     * 텍스트 추출 API
     *
     * 이미지 파일에서 텍스트 라인을 추출합니다.
     * Amazon Textract의 DetectDocumentText API를 사용하여 단순 텍스트만 추출합니다.
     * 테이블이나 폼 데이터가 필요한 경우 /analyze API를 사용하세요.
     *
     * @param file 텍스트를 추출할 이미지 파일 (PNG, JPG, JPEG, PDF)
     * @return 추출된 텍스트와 블록별 상세 정보
     */
    @Operation(
            summary = "이미지에서 텍스트 추출",
            description = "업로드된 이미지 파일에서 Amazon Textract를 사용하여 텍스트를 추출합니다. 지원 형식: PNG, JPG, JPEG, PDF"
    )
    @PostMapping(value = "/extract", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<ApiResponse<OcrResponse>> extractText(
            @Parameter(
                    description = "텍스트를 추출할 이미지 파일 (PNG, JPG, JPEG, PDF)",
                    required = true,
                    content = @Content(mediaType = MediaType.MULTIPART_FORM_DATA_VALUE)
            )
            @RequestParam("file") MultipartFile file
    ) {
        OcrResponse response = ocrService.extractText(file);
        return ResponseEntity.ok(ApiResponse.success(response, "텍스트 추출이 완료되었습니다"));
    }

    @PostMapping(value = "/python/extract", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<ApiResponse<OcrResponse>> extractTextWithPython(
            @RequestParam("file") MultipartFile file
    ) {
        OcrResponse response = ocrService.extractTextWithPython(file);
        return ResponseEntity.ok(ApiResponse.success(response, "텍스트 추출이 완료되었습니다"));
    }

    /**
     * 문서 분석 API (테이블/폼 추출)
     *
     * 이미지 파일에서 테이블 데이터와 키-값 쌍(폼 필드)을 구조화된 형태로 추출합니다.
     * Amazon Textract의 AnalyzeDocument API를 사용하여 TABLES, FORMS 피처를 분석합니다.
     *
     * 주문서, 송장, 영수증 등 구조화된 문서에서 특정 필드 값을 추출할 때 유용합니다.
     * 예: Order No, Product No, Supplier Name 등
     *
     * @param file 분석할 이미지 파일 (PNG, JPG, JPEG, PDF)
     * @return 테이블, 키-값 쌍, 폼 필드가 포함된 분석 결과
     */
    @Operation(
            summary = "문서 분석 (테이블/폼 추출)",
            description = """
                    업로드된 이미지 파일에서 Amazon Textract의 AnalyzeDocument API를 사용하여
                    테이블 데이터와 키-값 쌍(폼 필드)을 구조화된 형태로 추출합니다.

                    응답 구조:
                    - extractedText: 전체 텍스트
                    - lines: 텍스트 라인 목록
                    - tables: 테이블 목록 (각 테이블은 cells, rows, headerToFirstRowMap 포함)
                    - keyValuePairs: 키-값 쌍 목록 (Order No: 528003-1322 형태)
                    - formFields: 키-값을 Map으로 제공

                    지원 형식: PNG, JPG, JPEG, PDF
                    """
    )
    @PostMapping(value = "/analyze", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<ApiResponse<DocumentAnalysisResponse>> analyzeDocument(
            @Parameter(
                    description = "분석할 이미지 파일 (PNG, JPG, JPEG, PDF)",
                    required = true,
                    content = @Content(mediaType = MediaType.MULTIPART_FORM_DATA_VALUE)
            )
            @RequestParam("file") MultipartFile file
    ) {
        DocumentAnalysisResponse response = ocrService.analyzeDocument(file);
        return ResponseEntity.ok(ApiResponse.success(response, "문서 분석이 완료되었습니다"));
    }

    @PostMapping(value = "/python/analyze", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<ApiResponse<DocumentAnalysisResponse>> analyzeDocumentWithPython(
            @RequestParam("file") MultipartFile file
    ) {
        DocumentAnalysisResponse response = ocrService.analyzeDocumentWithPython(file);
        return ResponseEntity.ok(ApiResponse.success(response, "문서 분석이 완료되었습니다"));
    }
}
