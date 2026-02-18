package com.doosan.erp.ocr.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * 문서 분석 응답 DTO
 *
 * /api/v1/ocr/analyze API 응답 시 클라이언트에게 반환하는 데이터입니다.
 * Amazon Textract의 AnalyzeDocument API 결과를 담으며,
 * 테이블, 키-값 쌍(폼 필드) 등 구조화된 데이터를 포함합니다.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DocumentAnalysisResponse {

    // 추출된 전체 텍스트 (줄바꿈으로 연결)
    private String extractedText;

    // 텍스트 라인 목록
    private List<TextBlockDto> lines;

    // 추출된 테이블 목록
    private List<TableDto> tables;

    // 키-값 쌍 목록 (상세 정보 포함)
    private List<KeyValueDto> keyValuePairs;

    // 키-값을 Map으로 제공 (편의용)
    private Map<String, String> formFields;

    // 평균 신뢰도 (0.0 ~ 100.0)
    private Float averageConfidence;
}
