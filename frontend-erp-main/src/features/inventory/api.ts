/**
 * @file features/inventory/api.ts
 * @description 재고(Inventory) 관련 API 요청 함수 및 데이터 타입을 정의합니다.
 */
import { client } from '../../api/client';
import type { ApiResponse } from '../../types';

// 재고 정보 인터페이스
export interface Stock {
  id: number;
  itemCode: string; // 품목 코드
  itemName: string; // 품목명
  warehouseCode: string; // 창고 코드
  warehouseName: string; // 창고명
  onHandQuantity: number; // 현재고 수량
  reservedQuantity: number; // 예약된 수량 (출고 예정 등)
  availableQuantity: number; // 가용 재고 (현재고 - 예약)
  unit: string; // 단위 (EA, KG etc.)
  unitPrice?: number; // 단가
}

// 재고 생성(입고) 요청 데이터 구조
export interface StockCreateRequest {
  itemCode: string;
  itemName: string;
  warehouseCode: string;
  warehouseName: string;
  quantity: number;
  unit: string;
  unitPrice: number;
}

// 재고 관련 API 함수 모음
export const inventoryApi = {
  // 전체 재고 목록 조회
  getAll: async () => {
    const response = await client.get<ApiResponse<Stock[]>>('/api/v1/inventory/stocks');
    return response.data;
  },

  // 신규 재고 등록
  create: async (data: StockCreateRequest) => {
    const response = await client.post<ApiResponse<Stock>>('/api/v1/inventory/stocks', data);
    return response.data;
  },
};
