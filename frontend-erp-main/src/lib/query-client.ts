/**
 * @file lib/query-client.ts
 * @description React Query의 QueryClient 설정 파일입니다.
 * 데이터 캐싱, 재시도 로직 등 서버 상태 관리의 기본 동작을 정의합니다.
 */
import { QueryClient } from '@tanstack/react-query';

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1, // API 요청 실패 시 1회 재시도
      refetchOnWindowFocus: false, // 윈도우 포커스 시 자동 재요청 비활성화
    },
  },
});
