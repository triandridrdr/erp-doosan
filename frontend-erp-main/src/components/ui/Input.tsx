/**
 * @file components/ui/Input.tsx
 * @description 재사용 가능한 입력 필드(Input) 컴포넌트입니다.
 * 레이블, 에러 메시지, 좌측 아이콘(leftIcon) 표시 기능을 지원합니다.
 */
import * as React from 'react';

import { cn } from '../../lib/utils';

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string; // 입력 필드 상단 레이블
  error?: string; // 유효성 검증 실패 시 표시할 에러 메시지
  leftIcon?: React.ReactNode; // 입력 필드 내부에 표시할 아이콘 (왼쪽)
}

// React.forwardRef를 사용하여 React Hook Form과 통합 가능하도록 함
export const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({ className, label, error, leftIcon, ...props }, ref) => {
    return (
      <div className='w-full space-y-2'>
        {/* 레이블 표시 */}
        {label && <label className='text-sm font-semibold text-gray-700'>{label}</label>}

        <div className='relative group'>
          {/* 아이콘 표시 (존재할 경우) */}
          {leftIcon && (
            <div className='absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 group-focus-within:text-primary transition-colors duration-200'>
              {leftIcon}
            </div>
          )}

          <input
            className={cn(
              // 기본 스타일
              'flex h-12 w-full rounded-xl border border-gray-200 bg-gray-50/50 px-3 py-2 text-sm text-gray-900 placeholder:text-gray-400 focus:bg-white focus:outline-none focus:ring-2 focus:ring-primary/20 focus:border-primary transition-all duration-200',
              // 아이콘이 있을 경우 왼쪽 패딩 추가
              leftIcon && 'pl-10',
              // 에러가 있을 경우 빨간색 테두리와 배경 적용
              error && 'border-red-500 focus:ring-red-500 bg-red-50/10',
              className,
            )}
            ref={ref}
            {...props}
          />
        </div>

        {/* 에러 메시지 표시 */}
        {error && <p className='text-sm text-red-500 font-medium animate-fade-in'>{error}</p>}
      </div>
    );
  },
);
Input.displayName = 'Input';
