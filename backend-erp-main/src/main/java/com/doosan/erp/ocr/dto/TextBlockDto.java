package com.doosan.erp.ocr.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 텍스트 블록 DTO
 *
 * Amazon Textract에서 추출한 개별 텍스트 블록 정보입니다.
 * 주로 LINE 타입 블록을 담으며, 텍스트 내용과 신뢰도를 포함합니다.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TextBlockDto {

    // 블록의 텍스트 내용
    private String text;

    // 텍스트 인식 신뢰도 (0.0 ~ 100.0)
    private Float confidence;

    // 블록 타입 (LINE, WORD 등)
    private String blockType;
}
