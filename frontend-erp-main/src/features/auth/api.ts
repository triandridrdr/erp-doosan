/**
 * @file features/auth/api.ts
 * @description 인증 관련 API 요청 함수 및 데이터 타입을 정의합니다.
 */
import { client } from '../../api/client';
import type { ApiResponse, User } from '../../types';

// 로그인 요청 데이터 구조
export interface LoginRequest {
  userId: string; // 사용자 ID (이메일이 아닌 ID 사용)
  password: string;
}

// 로그인 응답 데이터 구조
export interface LoginResponse {
  accessToken: string; // JWT 접근 토큰
  tokenType: string; // 토큰 타입 (보통 Bearer)
  expiresIn: number; // 만료 시간
}

// 회원가입 요청 데이터 구조
export interface SignupRequest {
  userId: string;
  name: string;
  password: string;
}

// 인증 관련 API 함수 모음
export const authApi = {
  /**
   * 로그인 요청을 보냅니다.
   * 성공 시 액세스 토큰을 포함한 응답을 반환합니다.
   */
  login: async (data: LoginRequest) => {
    const response = await client.post<ApiResponse<LoginResponse>>('/api/auth/login', data);
    return response.data;
  },

  /**
   * 회원가입 요청을 보냅니다.
   */
  signup: async (data: SignupRequest) => {
    const response = await client.post<ApiResponse<number>>('/api/auth/signup', data);
    return response.data;
  },

  /**
   * (선택 사항) 현재 로그인한 사용자 정보를 가져옵니다.
   * 필요 시 사용될 수 있습니다.
   */
  getProfile: async () => {
    const response = await client.get<ApiResponse<User>>('/api/auth/me'); // 예시 엔드포인트
    return response.data;
  },
};
