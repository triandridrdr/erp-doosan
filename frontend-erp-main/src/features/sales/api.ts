/**
 * @file features/sales/api.ts
 * @description 영업(Sales) 관련 API 요청 함수 및 데이터 타입을 정의합니다.
 */
import { client } from '../../api/client';
import type { ApiResponse } from '../../types';

// 주문 상태 상수 정의
export const OrderStatus = {
  PENDING: 'PENDING', // 대기 중
  CONFIRMED: 'CONFIRMED', // 승인됨
  SHIPPED: 'SHIPPED', // 배송됨
  CANCELLED: 'CANCELLED', // 취소됨
} as const;

export type OrderStatus = (typeof OrderStatus)[keyof typeof OrderStatus];

// 주문 품목(Line Item) 인터페이스
export interface SalesOrderLine {
  id: number;
  lineNumber: number; // 품목 순번
  itemCode: string; // 품목 코드
  itemName: string; // 품목명
  quantity: number; // 수량
  unitPrice: number; // 단가
  lineAmount: number; // 총액 (수량 * 단가)
  remarks?: string; // 비고
}

// 판매 주문(Sales Order) 인터페이스
export interface SalesOrder {
  id: number;
  orderNumber: string; // 주문 번호
  orderDate: string; // 주문 일자 (ISO Date String)
  customerCode: string; // 고객사 코드
  customerName: string; // 고객사명
  status: OrderStatus; // 주문 상태
  totalAmount: number; // 총 주문 금액
  deliveryAddress?: string; // 배송지 주소
  remarks?: string; // 비고
  lines: SalesOrderLine[]; // 주문 품목 목록
  createdAt: string; // 생성 일시
  createdBy: string; // 생성자
}

// 주문 생성 요청 데이터 구조
export interface SalesOrderRequest {
  orderDate: string; // yyyy-MM-dd
  customerCode: string;
  customerName: string;
  deliveryAddress?: string;
  remarks?: string;
  lines: {
    lineNumber: number;
    itemCode: string;
    itemName: string;
    quantity: number;
    unitPrice: number;
    remarks?: string;
  }[];
}

// 영업 관련 API 함수 모음
export const salesApi = {
  // 모든 판매 주문 조회
  getAll: async () => {
    // 백엔드는 페이징된 응답 구조를 반환함 (content 배열 포함)
    const response = await client.get<ApiResponse<{ content: SalesOrder[] }>>('/api/v1/sales/orders');
    return response.data; // { success: true, data: { content: [...] } }
  },
  // 특정 판매 주문 상세 조회
  getOne: async (id: number) => {
    const response = await client.get<ApiResponse<SalesOrder>>(`/api/v1/sales/orders/${id}`);
    return response.data;
  },
  // 신규 판매 주문 생성
  create: async (data: SalesOrderRequest) => {
    const response = await client.post<ApiResponse<SalesOrder>>('/api/v1/sales/orders', data);
    return response.data;
  },
};
