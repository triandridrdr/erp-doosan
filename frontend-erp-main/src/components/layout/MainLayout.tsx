/**
 * @file components/layout/MainLayout.tsx
 * @description 애플리케이션의 메인 레이아웃 컴포넌트입니다.
 * 사이드바와 헤더를 포함하며, 콘텐츠 영역에 자식 라우트(Outlet)를 렌더링합니다.
 */
import { Outlet, useLocation } from 'react-router-dom';

import { Sidebar } from './Sidebar';

export function MainLayout() {
  const location = useLocation();

  // 현재 경로에 따라 헤더 제목을 동적으로 설정합니다.
  const pageTitle =
    {
      '/': 'Dashboard',
      '/sales': 'Sales Orders',
      '/inventory': 'Inventory',
      '/accounting': 'Accounting',
      '/ocr': 'OCR Extraction',
      '/ocr-python': 'OCR with Python',
    }[location.pathname] || 'Overview';

  return (
    <div className='min-h-screen bg-slate-50 font-sans text-gray-900'>
      {/* 1. 사이드바 (왼쪽 고정 네비게이션) */}
      <Sidebar />

      {/* 2. 메인 콘텐츠 영역 (사이드바 너비만큼 왼쪽 여백 적용) */}
      <div className='pl-72 flex flex-col min-h-screen transition-all duration-300'>
        {/* 2.1 헤더: 페이지 제목 및 상단 우측 기능 (알림 등) */}
        <header className='h-20 bg-white/80 backdrop-blur-xl border-b border-gray-200 sticky top-0 z-20 px-10 flex items-center justify-between'>
          <div>
            <h2 className='text-2xl font-bold text-gray-800 tracking-tight'>{pageTitle}</h2>
            <p className='text-sm text-gray-500 mt-1'>Check your work status for today</p>
          </div>

          <div className='flex items-center gap-4'>
            {/* 알림 버튼 예시 */}
            <button className='p-2 rounded-full hover:bg-gray-100 text-gray-500 transition-colors relative'>
              <span className='absolute top-2 right-2 w-2 h-2 bg-red-500 rounded-full animate-pulse ring-2 ring-white'></span>
              <svg className='w-6 h-6' fill='none' viewBox='0 0 24 24' stroke='currentColor'>
                <path
                  strokeLinecap='round'
                  strokeLinejoin='round'
                  strokeWidth={2}
                  d='M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9'
                />
              </svg>
            </button>
          </div>
        </header>

        {/* 2.2 메인 콘텐츠: 라우트별 페이지 내용이 렌더링되는 곳 */}
        <main className='flex-1 p-10 animate-fade-in'>
          <div className='max-w-7xl mx-auto'>
            <Outlet />
          </div>
        </main>
      </div>
    </div>
  );
}
