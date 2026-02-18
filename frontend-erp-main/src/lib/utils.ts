/**
 * @file lib/utils.ts
 * @description 프로젝트 전반에서 사용되는 유틸리티 함수 모음입니다.
 */
import { type ClassValue, clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

/**
 * Tailwind CSS 클래스를 병합하는 유틸리티 함수입니다.
 * clsx로 조건부 클래스를 결합하고, twMerge로 충돌하는 Tailwind 클래스를 정리합니다.
 * @param inputs - 병합할 클래스 목록
 */
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}
