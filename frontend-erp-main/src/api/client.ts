/**
 * @file api/client.ts
 * @description Axios를 사용한 API 클라이언트 인스턴스 설정 파일입니다.
 * 인터셉터를 통해 요청 헤더에 토큰을 추가하고, 응답 에러를 전역적으로 처리합니다.
 */
import axios from 'axios';

// 환경 변수에서 API 기본 URL을 가져옵니다.
const BASE_URL = import.meta.env.VITE_API_URL;

// Axios 인스턴스 생성
export const client = axios.create({
  baseURL: BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

/**
 * 요청 인터셉터 (Request Interceptor)
 * 모든 API 요청 헤더에 로컬 스토리지에 저장된 인증 토큰을 자동으로 추가합니다.
 */
client.interceptors.request.use((config) => {
  const token = localStorage.getItem('token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

/**
 * 응답 인터셉터 (Response Interceptor)
 * API 응답 에러 중 401(Unauthorized) 또는 403(Forbidden) 발생 시
 * 세션을 종료하고 로그인 페이지로 이동시킵니다.
 */
client.interceptors.response.use(
  (response) => response,
  (error) => {
    // 로그인/회원가입 요청에서 발생한 에러는 리다이렉트 처리하지 않음
    const isAuthRequest = error.config?.url?.includes('/auth/login') || error.config?.url?.includes('/auth/signup');

    if ((error.response?.status === 401 || error.response?.status === 403) && !isAuthRequest) {
      // 토큰 및 사용자 정보 삭제
      localStorage.removeItem('token');
      localStorage.removeItem('user');

      // 로그인 페이지로 강제 이동
      window.location.href = '/login';
    }
    return Promise.reject(error);
  },
);
