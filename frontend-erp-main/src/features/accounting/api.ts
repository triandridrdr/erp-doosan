/**
 * @file features/accounting/api.ts
 * @description 회계(Accounting) 관련 API 요청 함수 및 데이터 타입을 정의합니다.
 */
import { client } from '../../api/client';
import type { ApiResponse } from '../../types';

// 분개장 라인(전표 상세 항목) 인터페이스
export interface JournalEntryLine {
  id: number;
  lineNumber: number; // 라인 순번
  accountCode: string; // 계정 코드
  accountName: string; // 계정명
  debit: number; // 차변 금액
  credit: number; // 대변 금액
  description?: string; // 적요
}

// 분개장(전표) 헤더 인터페이스
export interface JournalEntry {
  id: number;
  entryNumber: string; // 전표 번호
  entryDate: string; // 전표 일자
  status: string; // 상태 (승인/미승인 등)
  description?: string; // 전표 전체 적요
  totalDebit: number; // 차변 합계
  totalCredit: number; // 대변 합계
  lines: JournalEntryLine[]; // 전표 상세 라인 목록
}

// 전표 생성 요청 데이터 구조
export interface JournalEntryCreateRequest {
  entryDate: string;
  description: string;
  lines: JournalEntryLineRequest[];
}

// 전표 생성 시 상세 라인 요청 데이터 구조
export interface JournalEntryLineRequest {
  lineNumber: number;
  accountCode: string;
  accountName: string;
  debit: number;
  credit: number;
  description: string;
}

// 회계 관련 API 함수 모음
export const accountingApi = {
  // 전체 전표 목록 조회
  getAll: async () => {
    // 백엔드는 페이징된 응답 구조를 반환함
    const response = await client.get<ApiResponse<{ content: JournalEntry[] }>>('/api/v1/accounting/journal-entries');
    return response.data; // { success: true, data: { content: [...] } }
  },

  // 신규 전표 생성
  create: async (data: JournalEntryCreateRequest) => {
    const response = await client.post<ApiResponse<JournalEntry>>('/api/v1/accounting/journal-entries', data);
    return response.data;
  },
};
