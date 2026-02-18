/**
 * @file features/ocr/types.ts
 * @description OCR 및 문서 분석 결과에 대한 데이터 타입(DTO)을 정의합니다.
 */

// ----- 단순 텍스트 추출 (Extract) 관련 타입 -----

// 텍스트 블록
export interface TextBlockDto {
  text: string; // 추출된 텍스트
  confidence: number; // 신뢰도 (0~100)
  blockType: string; // 블록 타입 (LINE, WORD 등)
}

// OCR 응답 데이터
export interface OcrResponseData {
  extractedText: string; // 전체 결합 텍스트
  blocks: TextBlockDto[]; // 개별 텍스트 블록 리스트
  averageConfidence: number; // 평균 신뢰도
}

// OCR API 응답 구조
export interface OcrResponse {
  success: boolean;
  data: OcrResponseData;
  message: string;
  timestamp: string;
}

// ----- 문서 분석 (Analyze) 관련 타입 -----

// 테이블 셀 정보
export interface CellDto {
  rowIndex: number; // 행 인덱스
  columnIndex: number; // 열 인덱스
  text: string; // 셀 텍스트
  confidence: number; // 신뢰도
  header: boolean; // 헤더 여부
}

// 테이블 정보
export interface TableDto {
  tableIndex: number;
  rowCount: number; // 전체 행 수
  columnCount: number; // 전체 열 수
  cells: CellDto[]; // 셀 목록
  rows: string[][]; // 2차원 배열 형태의 데이터 (간편 조회용)
  headerToFirstRowMap: Record<string, string>; // 헤더 매핑 정보
}

// 폼 키-값 쌍 정보
export interface KeyValueDto {
  key: string; // 감지된 키 (예: "Name:")
  value: string; // 감지된 값 (예: "John Doe")
  keyConfidence: number;
  valueConfidence: number;
}

// 문서 분석 응답 데이터
export interface DocumentAnalysisResponseData {
  extractedText: string;
  lines: TextBlockDto[];
  tables: TableDto[]; // 감지된 테이블 목록
  keyValuePairs: KeyValueDto[]; // 감지된 키-값 쌍 목록
  formFields: Record<string, string>; // 단순화된 폼 필드 맵
  averageConfidence: number;
}

// 문서 분석 API 응답 구조
export interface DocumentAnalysisResponse {
  success: boolean;
  data: DocumentAnalysisResponseData;
  message: string;
  timestamp: string;
}
