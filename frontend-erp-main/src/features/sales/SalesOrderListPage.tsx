/**
 * @file features/sales/SalesOrderListPage.tsx
 * @description Page component to fetch and display sales orders.
 * Uses React Query and renders a table view.
 */
import { useQuery } from '@tanstack/react-query';
import { Plus, Search } from 'lucide-react';
import { useState } from 'react';

import { Button } from '../../components/ui/Button';
import { Input } from '../../components/ui/Input';
import { cn } from '../../lib/utils';
import { OrderStatus, salesApi } from './api';
import { SalesOrderCreateModal } from './SalesOrderCreateModal';

export function SalesOrderListPage() {
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false); // Create modal state

  // Fetch sales orders (React Query)
  const {
    data: sales,
    isLoading,
    error,
  } = useQuery({
    queryKey: ['sales-orders'], // Query key
    queryFn: async () => {
      const res = await salesApi.getAll();
      return res.data;
    },
  });

  // Returns badge style by order status
  const getStatusColor = (status: OrderStatus) => {
    switch (status) {
      case OrderStatus.CONFIRMED:
        return 'bg-green-100 text-green-800';
      case OrderStatus.PENDING:
        return 'bg-yellow-100 text-yellow-800';
      case OrderStatus.SHIPPED:
        return 'bg-blue-100 text-blue-800';
      case OrderStatus.CANCELLED:
        return 'bg-red-100 text-red-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  return (
    <div className='space-y-6'>
      {/* Header: title and create button */}
      <div className='flex items-center justify-between'>
        <h1 className='text-2xl font-bold text-gray-900'>Sales Orders</h1>
        <Button onClick={() => setIsCreateModalOpen(true)}>
          <Plus className='w-4 h-4 mr-2' />
          New order
        </Button>
      </div>

      {/* Search and filters */}
      <div className='bg-white p-4 rounded-lg shadow-sm border border-gray-200 flex items-center space-x-4'>
        <div className='relative flex-1 max-w-sm'>
          <Search className='absolute left-3 top-2.5 h-4 w-4 text-gray-400' />
          <Input placeholder='Search order number or customer name...' className='pl-9' />
        </div>
      </div>

      {/* Orders table */}
      <div className='bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden'>
        <div className='overflow-x-auto'>
          <table className='min-w-full divide-y divide-gray-200'>
            <thead className='bg-gray-50'>
              <tr>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>
                  Order No.
                </th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>
                  Customer
                </th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>
                  Order date
                </th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>Total</th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>Status</th>
              </tr>
            </thead>
            <tbody className='bg-white divide-y divide-gray-200'>
              {/* Loading state */}
              {isLoading && (
                <tr>
                  <td colSpan={5} className='px-6 py-10 text-center text-gray-500'>
                    Loading...
                  </td>
                </tr>
              )}
              {/* Error state */}
              {error && (
                <tr>
                  <td colSpan={5} className='px-6 py-10 text-center text-red-500'>
                    An error occurred while loading data.
                  </td>
                </tr>
              )}
              {/* Empty state */}
              {sales?.content && sales.content.length === 0 && (
                <tr>
                  <td colSpan={5} className='px-6 py-10 text-center text-gray-500'>
                    No orders found.
                  </td>
                </tr>
              )}
              {/* Render rows */}
              {sales?.content?.map((order) => (
                <tr key={order.id} className='hover:bg-gray-50 cursor-pointer transition-colors'>
                  <td className='px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900'>{order.orderNumber}</td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-gray-500'>{order.customerName}</td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-gray-500'>{order.orderDate}</td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-gray-900 font-medium'>
                    {new Intl.NumberFormat('ko-KR', {
                      style: 'currency',
                      currency: 'KRW',
                    }).format(order.totalAmount)}
                  </td>
                  <td className='px-6 py-4 whitespace-nowrap'>
                    <span
                      className={cn('px-2.5 py-0.5 rounded-full text-xs font-medium', getStatusColor(order.status))}
                    >
                      {order.status}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Create order modal */}
      <SalesOrderCreateModal isOpen={isCreateModalOpen} onClose={() => setIsCreateModalOpen(false)} />
    </div>
  );
}
