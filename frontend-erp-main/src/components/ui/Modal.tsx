/**
 * @file components/ui/Modal.tsx
 * @description 화면 중앙에 오버레이와 함께 표시되는 모달 다이얼로그 컴포넌트입니다.
 * React Portals를 사용하여 document.body 하위에 렌더링됩니다.
 */
import { X } from 'lucide-react';
import React, { useEffect } from 'react';
import { createPortal } from 'react-dom';

interface ModalProps {
  isOpen: boolean; // 모달 표시 여부
  onClose: () => void; // 닫기 이벤트 핸들러
  title: string; // 모달 제목
  children: React.ReactNode; // 모달 내부 콘텐츠
}

export function Modal({ isOpen, onClose, title, children }: ModalProps) {
  // 모달이 열려있을 때 배경 스크롤 방지
  useEffect(() => {
    if (isOpen) {
      document.body.style.overflow = 'hidden';
    } else {
      document.body.style.overflow = 'unset';
    }
    return () => {
      document.body.style.overflow = 'unset';
    };
  }, [isOpen]);

  if (!isOpen) return null;

  // React Portals: 컴포넌트 계층 구조 바깥(body)에 렌더링
  return createPortal(
    <div className='fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/50 backdrop-blur-sm animate-in fade-in duration-200'>
      <div
        className='bg-white rounded-xl shadow-xl w-full max-w-2xl max-h-[90vh] flex flex-col animate-in zoom-in-95 duration-200'
        role='dialog'
        aria-modal='true'
      >
        {/* 모달 헤더 */}
        <div className='flex items-center justify-between p-4 border-b border-gray-100'>
          <h2 className='text-xl font-bold text-gray-900'>{title}</h2>
          <button
            onClick={onClose}
            className='p-1 rounded-full hover:bg-gray-100 transition-colors text-gray-500 hover:text-gray-700'
            aria-label='Close modal'
          >
            <X className='w-5 h-5' />
          </button>
        </div>

        {/* 모달 본문 (스크롤 가능) */}
        <div className='flex-1 overflow-y-auto p-6'>{children}</div>
      </div>
    </div>,
    document.body,
  );
}
