/**
 * @file App.tsx
 * @description 애플리케이션의 최상위 컴포넌트입니다.
 * 전역 상태 관리자(React Query, Auth)와 라우터를 설정합니다.
 */
import { QueryClientProvider } from '@tanstack/react-query';

import { AuthProvider } from './features/auth/AuthContext';
import { queryClient } from './lib/query-client';
import { Routes } from './routes';

function App() {
  return (
    // QueryClientProvider: 서버 상태 관리를 위한 React Query 클라이언트를 제공합니다.
    <QueryClientProvider client={queryClient}>
      {/* AuthProvider: 로그인 상태 등 인증 관련 데이터를 전역으로 공급합니다. */}
      <AuthProvider>
        {/* Routes: URL 경로에 따라 페이지를 렌더링하는 라우터 컴포넌트입니다. */}
        <Routes />
      </AuthProvider>
    </QueryClientProvider>
  );
}

export default App;
