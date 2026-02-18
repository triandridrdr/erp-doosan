/**
 * @file features/ocr/api.ts
 * @description OCR(광학 문자 인식) 관련 API 요청 함수를 정의합니다.
 */
import { client } from '../../api/client';
import type { DocumentAnalysisResponse, OcrResponse } from './types';

export const ocrApi = {
  /**
   * 단순 텍스트 추출 (Extract) API 호출
   * 이미지를 업로드하여 텍스트 블록들을 추출합니다.
   * @param file 업로드할 이미지 파일
   */
  extract: async (file: File) => {
    const formData = new FormData();
    formData.append('file', file);

    const response = await client.post<OcrResponse>('/api/v1/ocr/extract', formData, {
      headers: {
        'Content-Type': 'multipart/form-data', // 파일 업로드를 위한 헤더 설정
      },
    });
    return response.data;
  },

  /**
   * 문서 분석 (Analyze) API 호출
   * 이미지를 업로드하여 텍스트뿐만 아니라 테이블, 키-값 쌍(Form Data) 등을 구조화하여 추출합니다.
   * @param file 업로드할 이미지/PDF 파일
   */
  analyze: async (file: File) => {
    const formData = new FormData();
    formData.append('file', file);

    const response = await client.post<DocumentAnalysisResponse>('/api/v1/ocr/analyze', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
    return response.data;
  },
};
