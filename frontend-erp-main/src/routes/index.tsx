/**
 * @file routes/index.tsx
 * @description 애플리케이션의 라우팅 구성을 정의하는 파일입니다.
 * 페이지별 경로와 접근 권한(보호된 라우트)을 설정합니다.
 */
import { createBrowserRouter, Navigate, RouterProvider } from 'react-router-dom';

import { MainLayout } from '../components/layout/MainLayout';
import { JournalEntryListPage } from '../features/accounting/JournalEntryListPage';
import { useAuth } from '../features/auth/AuthContext';
import { LoginPage } from '../features/auth/LoginPage';
import { SignupPage } from '../features/auth/SignupPage';
import { StockListPage } from '../features/inventory/StockListPage';
import { OcrPage } from '../features/ocr/OcrPage';
import { SalesOrderListPage } from '../features/sales/SalesOrderListPage';

/**
 * 보호된 라우트 (Protected Route) 래퍼 컴포넌트
 * 인증되지 않은 사용자의 접근을 차단하고 로그인 페이지로 리다이렉트합니다.
 */
function ProtectedRoute() {
  const { isAuthenticated, isLoading } = useAuth();

  // Show loading state while checking authentication
  if (isLoading) return <div>Loading...</div>;

  // Redirect to login if not authenticated
  if (!isAuthenticated) {
    return <Navigate to='/login' replace />;
  }

  // Render main layout (with sidebar) if authenticated
  return <MainLayout />;
}

/**
 * 대시보드 컴포넌트
 * 메인 페이지의 현황판을 표시합니다.
 */
const Dashboard = () => (
  <div className='space-y-8'>
    {/* 요약 통계 그리드 */}
    <div className='grid grid-cols-1 md:grid-cols-3 gap-6'>
      {/* 통계 카드 1: 총 수주액 */}
      <div className='bg-white p-6 rounded-2xl shadow-sm border border-gray-100 hover:shadow-md transition-shadow duration-200 group'>
        <div className='flex items-center justify-between mb-4'>
          <div className='p-3 bg-indigo-50 rounded-xl group-hover:bg-indigo-100 transition-colors'>
            <svg className='w-6 h-6 text-indigo-600' fill='none' viewBox='0 0 24 24' stroke='currentColor'>
              <path
                strokeLinecap='round'
                strokeLinejoin='round'
                strokeWidth={2}
                d='M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z'
              />
            </svg>
          </div>
          <span className='text-xs font-semibold px-2.5 py-0.5 rounded-full bg-green-100 text-green-700'>+12.5%</span>
        </div>
        <h3 className='text-gray-500 text-sm font-medium'>Total sales orders</h3>
        <p className='text-3xl font-bold text-gray-900 mt-2'>₩ 124,500,000</p>
        <p className='text-sm text-gray-400 mt-1'>Up from last month</p>
      </div>

      {/* 통계 카드 2: 재고 현황 */}
      <div className='bg-white p-6 rounded-2xl shadow-sm border border-gray-100 hover:shadow-md transition-shadow duration-200 group'>
        <div className='flex items-center justify-between mb-4'>
          <div className='p-3 bg-blue-50 rounded-xl group-hover:bg-blue-100 transition-colors'>
            <svg className='w-6 h-6 text-blue-600' fill='none' viewBox='0 0 24 24' stroke='currentColor'>
              <path
                strokeLinecap='round'
                strokeLinejoin='round'
                strokeWidth={2}
                d='M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4'
              />
            </svg>
          </div>
          <span className='text-xs font-semibold px-2.5 py-0.5 rounded-full bg-gray-100 text-gray-600'>Stable</span>
        </div>
        <h3 className='text-gray-500 text-sm font-medium'>Inventory status</h3>
        <p className='text-3xl font-bold text-gray-900 mt-2'>
          1,234 <span className='text-lg font-normal text-gray-400'>Items</span>
        </p>
        <p className='text-sm text-gray-400 mt-1'>15 items are low on stock</p>
      </div>

      {/* 통계 카드 3: 미처리 주문 */}
      <div className='bg-white p-6 rounded-2xl shadow-sm border border-gray-100 hover:shadow-md transition-shadow duration-200 group'>
        <div className='flex items-center justify-between mb-4'>
          <div className='p-3 bg-amber-50 rounded-xl group-hover:bg-amber-100 transition-colors'>
            <svg className='w-6 h-6 text-amber-600' fill='none' viewBox='0 0 24 24' stroke='currentColor'>
              <path
                strokeLinecap='round'
                strokeLinejoin='round'
                strokeWidth={2}
                d='M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z'
              />
            </svg>
          </div>
          <span className='text-xs font-semibold px-2.5 py-0.5 rounded-full bg-red-100 text-red-700 animate-pulse'>
            Action Req
          </span>
        </div>
        <h3 className='text-gray-500 text-sm font-medium'>Pending orders</h3>
        <p className='text-3xl font-bold text-gray-900 mt-2'>
          12 <span className='text-lg font-normal text-gray-400'>items</span>
        </p>
        <p className='text-sm text-gray-400 mt-1'>2 are urgent orders</p>
      </div>
    </div>

    {/* 최근 활동 / 차트 플레이스홀더 */}
    <div className='grid grid-cols-1 lg:grid-cols-2 gap-6'>
      <div className='bg-white p-6 rounded-2xl shadow-sm border border-gray-100 h-80 flex flex-col items-center justify-center text-gray-400'>
        <p className='mb-2'>Sales trend chart</p>
        <div className='w-full h-40 bg-gray-50 rounded-xl animate-pulse'></div>
      </div>
      <div className='bg-white p-6 rounded-2xl shadow-sm border border-gray-100 h-80 flex flex-col items-center justify-center text-gray-400'>
        <p className='mb-2'>Recent orders</p>
        <div className='w-full h-full space-y-3 mt-4'>
          {[1, 2, 3].map((i) => (
            <div key={i} className='flex items-center justify-between p-3 bg-gray-50 rounded-lg'>
              <div className='h-2 w-24 bg-gray-200 rounded'></div>
              <div className='h-2 w-12 bg-gray-200 rounded'></div>
            </div>
          ))}
        </div>
      </div>
    </div>
  </div>
);

// 라우터 설정
const router = createBrowserRouter([
  {
    path: '/login', // 로그인 페이지
    element: <LoginPage />,
  },
  {
    path: '/signup', // 회원가입 페이지
    element: <SignupPage />,
  },
  {
    path: '/',
    element: <ProtectedRoute />, // 인증이 필요한 라우트 그룹
    children: [
      { index: true, element: <Dashboard /> }, // 기본 경로 (대시보드)
      { path: 'sales', element: <SalesOrderListPage /> }, // 영업 관리
      { path: 'inventory', element: <StockListPage /> }, // 재고 관리
      { path: 'accounting', element: <JournalEntryListPage /> }, // 회계 관리
      { path: 'ocr', element: <OcrPage /> }, // OCR 기능
    ],
  },
  {
    path: '*', // 존재하지 않는 경로는 홈으로 리다이렉트
    element: <Navigate to='/' replace />,
  },
]);

/**
 * 라우터 제공 컴포넌트
 */
export function Routes() {
  return <RouterProvider router={router} />;
}
