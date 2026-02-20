package com.doosan.erp.ocr.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * OCR 텍스트 추출 응답 DTO
 *
 * /api/v1/ocr/extract API 응답 시 클라이언트에게 반환하는 데이터입니다.
 * Amazon Textract의 DetectDocumentText API 결과를 담습니다.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OcrResponse {

    // 추출된 전체 텍스트 (줄바꿈으로 연결)
    private String extractedText;

    // 텍스트 블록 목록 (LINE 타입만 포함)
    private List<TextBlockDto> blocks;

    // 평균 신뢰도 (0.0 ~ 100.0)
    private Float averageConfidence;

    // (Optional) 추출된 테이블 목록 (Python OCR 등에서 제공될 수 있음)
    private List<TableDto> tables;

    // (Optional) 키-값 필드 (Python OCR 등에서 제공될 수 있음)
    private Map<String, String> formFields;
}
