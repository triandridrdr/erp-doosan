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

export const ocrDraftApi = {
  save: async (req: { sourceFilename?: string; soNumber?: string; draft: unknown }) => {
    const response = await client.post('/api/v1/ocr/drafts', req);
    return response.data;
  },

  list: async () => {
    const response = await client.get('/api/v1/ocr/drafts');
    return response.data;
  },

  get: async (id: number) => {
    const response = await client.get(`/api/v1/ocr/drafts/${id}`);
    return response.data;
  },

  update: async (id: number, req: { sourceFilename?: string; soNumber?: string; draft: unknown }) => {
    const response = await client.put(`/api/v1/ocr/drafts/${id}`, req);
    return response.data;
  },

  approve: async (id: number) => {
    const response = await client.post(`/api/v1/ocr/drafts/${id}/approve`);
    return response.data;
  },

  delete: async (id: number) => {
    const response = await client.delete(`/api/v1/ocr/drafts/${id}`);
    return response.data;
  },
};

export const ocrPythonApi = {
  extract: async (file: File) => {
    const formData = new FormData();
    formData.append('file', file);

    const response = await client.post<OcrResponse>('/api/v1/ocr/python/extract?view=json', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
    return response.data;
  },

  analyze: async (file: File) => {
    const formData = new FormData();
    formData.append('file', file);

    const response = await client.post<DocumentAnalysisResponse>('/api/v1/ocr/python/analyze', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
    return response.data;
  },
};
