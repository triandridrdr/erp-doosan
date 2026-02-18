/**
 * @file components/layout/Sidebar.tsx
 * @description 애플리케이션의 사이드바 네비게이션 컴포넌트입니다.
 * 주요 페이지로 이동하는 링크 목록과 현재 로그인한 사용자 정보를 표시합니다.
 */
import { FileText, LayoutDashboard, LogOut, Package, ScanLine, ShoppingCart, User } from 'lucide-react';
import { NavLink } from 'react-router-dom';

import { useAuth } from '../../features/auth/AuthContext';
import { cn } from '../../lib/utils';

// 네비게이션 메뉴 항목 정의
const navigation = [
  { name: 'Dashboard', href: '/', icon: LayoutDashboard },
  { name: 'Sales Orders', href: '/sales', icon: ShoppingCart },
  { name: 'Inventory', href: '/inventory', icon: Package },
  { name: 'Accounting', href: '/accounting', icon: FileText },
  { name: 'OCR Extraction', href: '/ocr', icon: ScanLine },
];

export function Sidebar() {
  const { logout, user } = useAuth(); // 인증 컨텍스트에서 로그아웃 함수와 유저 정보 사용

  return (
    <div className='flex w-72 flex-col bg-slate-900 border-r border-slate-800 h-screen fixed left-0 top-0 z-30 shadow-2xl transition-all duration-300'>
      {/* 1. 로고 섹션 */}
      <div className='flex h-20 items-center px-8 border-b border-white/10'>
        <div className='w-10 h-10 bg-indigo-500 rounded-xl flex items-center justify-center shadow-lg shadow-indigo-500/30 mr-3'>
          <svg className='w-6 h-6 text-white' fill='none' viewBox='0 0 24 24' stroke='currentColor'>
            <path strokeLinecap='round' strokeLinejoin='round' strokeWidth={2} d='M13 10V3L4 14h7v7l9-11h-7z' />
          </svg>
        </div>
        <h1 className='text-xl font-bold text-white tracking-wide'>ERP Pro</h1>
      </div>

      {/* 2. 네비게이션 메뉴 */}
      <nav className='flex-1 space-y-2 px-4 py-8'>
        {navigation.map((item) => (
          <NavLink
            key={item.name}
            to={item.href}
            className={({ isActive }) =>
              cn(
                // 기본 스타일
                'group flex items-center px-4 py-3.5 text-sm font-medium rounded-xl transition-all duration-200 ease-in-out',
                // 활성화(현재 페이지) 상태 스타일
                isActive
                  ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-600/30 translate-x-1'
                  : 'text-slate-400 hover:bg-slate-800 hover:text-white hover:translate-x-1',
              )
            }
          >
            {({ isActive }) => (
              <>
                <item.icon
                  className={cn(
                    'mr-4 h-5 w-5 shrink-0 transition-colors duration-200',
                    isActive ? 'text-white' : 'text-slate-500 group-hover:text-white',
                  )}
                  aria-hidden='true'
                />
                {item.name}
              </>
            )}
          </NavLink>
        ))}
      </nav>

      {/* 3. 사용자 프로필 및 로그아웃 섹션 */}
      <div className='border-t border-white/10 p-6 bg-slate-900/50 backdrop-blur-sm'>
        <div className='flex items-center justify-between'>
          <div className='flex items-center min-w-0'>
            <div className='h-10 w-10 rounded-full bg-linear-to-tr from-indigo-500 to-purple-500 flex items-center justify-center shadow-lg ring-2 ring-white/10'>
              <User className='h-5 w-5 text-white' />
            </div>
            <div className='ml-3 min-w-0'>
              <p className='text-sm font-semibold text-white truncate'>{user?.name || 'User'}</p>
              <p className='text-xs text-slate-400 truncate'>Administrator</p>
            </div>
          </div>
          <button
            onClick={logout}
            className='p-2 rounded-lg text-slate-400 hover:text-white hover:bg-white/10 transition-colors'
            title='Logout'
          >
            <LogOut size={18} />
          </button>
        </div>
      </div>
    </div>
  );
}
