package com.doosan.erp.ocr.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 키-값 쌍 DTO
 *
 * Amazon Textract에서 추출한 폼 필드(레이블-값 쌍) 정보입니다.
 * 예: "Order No:" → "528003-1322"
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KeyValueDto {

    // 키 (레이블)
    private String key;

    // 값
    private String value;

    // 키 인식 신뢰도 (0.0 ~ 100.0)
    private Float keyConfidence;

    // 값 인식 신뢰도 (0.0 ~ 100.0)
    private Float valueConfidence;
}
