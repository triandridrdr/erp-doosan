/**
 * @file types/index.ts
 * @description 프로젝트 전반에서 공통으로 사용되는 타입 정의 파일입니다.
 */

/**
 * 공통 API 응답 구조 인터페이스
 * @template T - 응답 데이터(data)의 타입
 */
export interface ApiResponse<T> {
  success: boolean; // 요청 성공 여부
  data: T; // 실제 응답 데이터
  error?: {
    // 실패 시 에러 정보
    code: string;
    message: string;
  };
}

/**
 * 사용자 정보 인터페이스
 */
export interface User {
  id: number;
  userId: string;
  name: string;
  role: 'USER' | 'ADMIN'; // 사용자 권한 역할
}
