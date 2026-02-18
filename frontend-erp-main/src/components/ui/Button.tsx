/**
 * @file components/ui/Button.tsx
 * @description 재사용 가능한 버튼 컴포넌트입니다.
 * 다양한 스타일(variant)과 크기(size)를 지원하며, 로딩 상태를 표시할 수 있습니다.
 */
import * as React from 'react';

import { cn } from '../../lib/utils';

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'outline' | 'ghost' | 'danger'; // 버튼 스타일 종류
  size?: 'sm' | 'md' | 'lg'; // 버튼 크기
  isLoading?: boolean; // 로딩 중 여부 (스피너 표시 및 클릭 방지)
}

// React.forwardRef를 사용하여 부모 컴포넌트에서 DOM 요소(button)에 접근할 수 있도록 함
export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant = 'primary', size = 'md', isLoading, children, ...props }, ref) => {
    // 기본 스타일 정의
    const baseStyles =
      'inline-flex items-center justify-center rounded-lg font-medium transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-offset-2 disabled:opacity-50 disabled:pointer-events-none cursor-pointer active:scale-95';

    // 변형(Variant)별 스타일
    const variants = {
      primary: 'bg-indigo-600 text-white font-bold hover:bg-indigo-700 shadow-md hover:shadow-lg focus:ring-indigo-600',
      secondary: 'bg-slate-500 text-white font-medium hover:bg-slate-600 shadow-sm hover:shadow focus:ring-slate-500',
      outline:
        'border-2 border-gray-200 bg-transparent font-medium hover:bg-gray-50 text-gray-700 hover:border-gray-300',
      ghost: 'hover:bg-gray-100 text-gray-700 hover:text-gray-900',
      danger: 'bg-red-600 text-white font-medium hover:bg-red-700 shadow-sm focus:ring-red-600',
    };

    // 크기(Size)별 스타일
    const sizes = {
      sm: 'h-8 px-3 text-xs',
      md: 'h-10 px-4 py-2 text-sm',
      lg: 'h-12 px-6 text-base',
    };

    return (
      <button
        ref={ref}
        // cn 함수를 사용하여 클래스 병합 (base + variant + size + custom className)
        className={cn(baseStyles, variants[variant], sizes[size], className)}
        disabled={isLoading || props.disabled}
        {...props}
      >
        {/* 로딩 상태일 때 스피너 표시 */}
        {isLoading ? (
          <svg
            className='animate-spin -ml-1 mr-2 h-4 w-4 text-current'
            xmlns='http://www.w3.org/2000/svg'
            fill='none'
            viewBox='0 0 24 24'
          >
            <circle className='opacity-25' cx='12' cy='12' r='10' stroke='currentColor' strokeWidth='4'></circle>
            <path
              className='opacity-75'
              fill='currentColor'
              d='M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z'
            ></path>
          </svg>
        ) : null}
        {children}
      </button>
    );
  },
);
Button.displayName = 'Button';
