import { client } from '../../api/client';
import type { ApiResponse } from '../../types';

export interface PageResponse<T> {
  content: T[];
  page: number;
  size: number;
  totalElements: number;
  totalPages: number;
  first: boolean;
  last: boolean;
}

export interface Style {
  id: number;
  productId?: string;
  styleCode: string;
  styleName: string;
  season?: string;
  description?: string;
  defaultBomMasterId?: number;
}

export interface StyleRequest {
  productId?: string;
  styleCode: string;
  styleName: string;
  season?: string;
  description?: string;
  defaultBomMasterId?: number;
}

export type BomLine = {
  lineNo: number;
  component?: string;
  category?: string;
  composition?: string;
  uom?: string;
  consumptionPerUnit?: string;
  wastePercent?: string;
};

export const styleApi = {
  list: async (params?: { page?: number; size?: number; search?: string }) => {
    const response = await client.get<ApiResponse<PageResponse<Style>>>('/api/v1/styles', {
      params: {
        page: params?.page ?? 0,
        size: params?.size ?? 20,
        search: params?.search,
      },
    });
    return response.data;
  },
  getOne: async (id: number) => {
    const response = await client.get<ApiResponse<Style>>(`/api/v1/styles/${id}`);
    return response.data;
  },
  getDefaultBomLines: async (id: number) => {
    const response = await client.get<ApiResponse<BomLine[]>>(`/api/v1/styles/${id}/default-bom-lines`);
    return response.data;
  },
  create: async (data: StyleRequest) => {
    const response = await client.post<ApiResponse<Style>>('/api/v1/styles', data);
    return response.data;
  },
  update: async (id: number, data: StyleRequest) => {
    const response = await client.put<ApiResponse<Style>>(`/api/v1/styles/${id}`, data);
    return response.data;
  },
  delete: async (id: number) => {
    const response = await client.delete<ApiResponse<void>>(`/api/v1/styles/${id}`);
    return response.data;
  },
};
