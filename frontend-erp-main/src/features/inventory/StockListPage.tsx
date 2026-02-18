/**
 * @file features/inventory/StockListPage.tsx
 * @description Page component to fetch and display inventory stock.
 * Shows on-hand and available quantities by item and warehouse.
 */
import { useQuery } from '@tanstack/react-query';
import { Plus, RotateCw, Search } from 'lucide-react';
import { useState } from 'react';

import { Button } from '../../components/ui/Button';
import { Input } from '../../components/ui/Input';
import { inventoryApi } from './api';
import { StockCreateModal } from './StockCreateModal';

export function StockListPage() {
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false); // Create modal state

  // Fetch stock list
  const {
    data: stocks,
    isLoading,
    refetch,
  } = useQuery({
    queryKey: ['stocks'],
    queryFn: async () => {
      const res = await inventoryApi.getAll();
      return res.data;
    },
  });

  return (
    <div className='space-y-6'>
      {/* Header: title and actions (create, refresh) */}
      <div className='flex items-center justify-between'>
        <h1 className='text-2xl font-bold text-gray-900'>Inventory</h1>
        <div className='flex gap-2'>
          <Button onClick={() => setIsCreateModalOpen(true)}>
            <Plus className='w-4 h-4 mr-2' />
            New stock
          </Button>
          <Button variant='outline' onClick={() => refetch()}>
            <RotateCw className='w-4 h-4 mr-2' />
            Refresh
          </Button>
        </div>
      </div>

      {/* Search filters */}
      <div className='bg-white p-4 rounded-lg shadow-sm border border-gray-200 flex items-center space-x-4'>
        <div className='relative flex-1 max-w-sm'>
          <Search className='absolute left-3 top-2.5 h-4 w-4 text-gray-400' />
          <Input placeholder='Search item name or warehouse...' className='pl-9' />
        </div>
      </div>

      {/* Stock table */}
      <div className='bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden'>
        <div className='overflow-x-auto'>
          <table className='min-w-full divide-y divide-gray-200'>
            <thead className='bg-gray-50'>
              <tr>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>
                  Item code
                </th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>
                  Item name
                </th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>Warehouse</th>
                <th className='px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider'>
                  On hand
                </th>
                <th className='px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider'>
                  Available
                </th>
                <th className='px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider'>
                  Unit
                </th>
              </tr>
            </thead>
            <tbody className='bg-white divide-y divide-gray-200'>
              {/* Loading state */}
              {isLoading && (
                <tr>
                  <td colSpan={6} className='px-6 py-10 text-center text-gray-500'>
                    Loading...
                  </td>
                </tr>
              )}
              {/* Render rows */}
              {stocks?.map((stock) => (
                <tr key={stock.id} className='hover:bg-gray-50 transition-colors'>
                  <td className='px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900'>{stock.itemCode}</td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-gray-500'>{stock.itemName}</td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-gray-500'>{stock.warehouseName}</td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-gray-900 text-right font-medium'>
                    {stock.onHandQuantity}
                  </td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-blue-600 text-right font-bold'>
                    {stock.availableQuantity}
                  </td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-gray-500 text-center'>{stock.unit}</td>
                </tr>
              ))}
              {/* Empty state */}
              {stocks && stocks.length === 0 && (
                <tr>
                  <td colSpan={6} className='px-6 py-10 text-center text-gray-500'>
                    No stock data.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Create modal */}
      <StockCreateModal isOpen={isCreateModalOpen} onClose={() => setIsCreateModalOpen(false)} />
    </div>
  );
}
