package com.doosan.erp.ocr.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 테이블 셀 DTO
 *
 * Amazon Textract에서 추출한 테이블의 개별 셀 정보입니다.
 * 행/열 인덱스, 텍스트 내용, 신뢰도, 헤더 여부를 포함합니다.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CellDto {

    // 행 인덱스 (1부터 시작)
    private int rowIndex;

    // 열 인덱스 (1부터 시작)
    private int columnIndex;

    // 셀의 텍스트 내용
    private String text;

    // 텍스트 인식 신뢰도 (0.0 ~ 100.0)
    private Float confidence;

    // 헤더 셀 여부
    private boolean isHeader;
}
