package com.doosan.erp.ocr.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * 테이블 DTO
 *
 * Amazon Textract에서 추출한 테이블 데이터입니다.
 * 셀 목록, 2차원 배열 형태의 rows, 헤더-값 매핑을 제공합니다.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableDto {

    // 테이블 인덱스 (0부터 시작)
    private int tableIndex;

    // 행 개수
    private int rowCount;

    // 열 개수
    private int columnCount;

    // 셀 목록 (상세 정보 포함)
    private List<CellDto> cells;

    // 2차원 배열 형태의 테이블 데이터
    private List<List<String>> rows;

    // 헤더(첫 번째 행) - 첫 번째 데이터 행 값 매핑
    private Map<String, String> headerToFirstRowMap;
}
