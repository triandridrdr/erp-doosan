/**
 * @file features/sales/SalesOrderCreateModal.tsx
 * @description Modal component to create a new sales order.
 * Allows entering header fields and dynamically adding/removing line items.
 */
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Plus, Trash2 } from 'lucide-react';
import React, { useState } from 'react';

import { Button } from '../../components/ui/Button';
import { Input } from '../../components/ui/Input';
import { Modal } from '../../components/ui/Modal';
import { salesApi, type SalesOrderRequest } from './api';

interface Props {
  isOpen: boolean; // Modal open state
  onClose: () => void; // Close handler
}

// Initial line item
const initialLine = {
  itemCode: '',
  itemName: '',
  quantity: 1,
  unitPrice: 0,
  remarks: '',
};

export function SalesOrderCreateModal({ isOpen, onClose }: Props) {
  const queryClient = useQueryClient();

  // Header form state
  const [formData, setFormData] = useState<Partial<SalesOrderRequest>>({
    orderDate: new Date().toISOString().split('T')[0], // Default to today
    customerCode: '',
    customerName: '',
    deliveryAddress: '',
    remarks: '',
  });

  // Line items state
  const [lines, setLines] = useState([initialLine]);

  // Create mutation (React Query)
  const createMutation = useMutation({
    mutationFn: salesApi.create,
    onSuccess: () => {
      // Refresh list on success
      queryClient.invalidateQueries({ queryKey: ['sales-orders'] });
      onClose();
      // Reset form
      setFormData({
        orderDate: new Date().toISOString().split('T')[0],
        customerCode: '',
        customerName: '',
        deliveryAddress: '',
        remarks: '',
      });
      setLines([initialLine]);
      alert('Sales order created successfully.');
    },
    onError: (error: Error) => {
      alert(`Failed to create sales order: ${error.message}`);
    },
  });

  // Line item change handler
  const handleLineChange = (index: number, field: string, value: string | number) => {
    const newLines = [...lines];
    newLines[index] = { ...newLines[index], [field]: value };
    setLines(newLines);
  };

  // Add line item
  const addLine = () => {
    setLines([...lines, initialLine]);
  };

  // Remove line item (keep at least one)
  const removeLine = (index: number) => {
    if (lines.length > 1) {
      setLines(lines.filter((_, i) => i !== index));
    }
  };

  // Form submit handler
  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.customerCode || !formData.customerName || !formData.orderDate) {
      alert('Please fill in the required fields.');
      return;
    }

    // Build API payload
    const payload: SalesOrderRequest = {
      orderDate: formData.orderDate!,
      customerCode: formData.customerCode!,
      customerName: formData.customerName!,
      deliveryAddress: formData.deliveryAddress,
      remarks: formData.remarks,
      lines: lines.map((line, index) => ({
        ...line,
        lineNumber: index + 1, // Auto sequence
        quantity: Number(line.quantity),
        unitPrice: Number(line.unitPrice),
      })),
    };

    createMutation.mutate(payload);
  };

  // Calculate total
  const totalAmount = lines.reduce(
    (sum, line) => sum + (Number(line.quantity) || 0) * (Number(line.unitPrice) || 0),
    0,
  );

  return (
    <Modal isOpen={isOpen} onClose={onClose} title='New Sales Order'>
      <form onSubmit={handleSubmit} className='space-y-6'>
        {/* Header fields */}
        <div className='grid grid-cols-2 gap-4'>
          <div>
            <label className='block text-sm font-medium text-gray-700 mb-1'>
              Order date <span className='text-red-500'>*</span>
            </label>
            <Input
              type='date'
              value={formData.orderDate}
              onChange={(e) => setFormData({ ...formData, orderDate: e.target.value })}
              required
            />
          </div>
          <div>
            <label className='block text-sm font-medium text-gray-700 mb-1'>
              Customer code <span className='text-red-500'>*</span>
            </label>
            <Input
              value={formData.customerCode}
              onChange={(e) => setFormData({ ...formData, customerCode: e.target.value })}
              placeholder='CUST-001'
              required
            />
          </div>
          <div className='col-span-2'>
            <label className='block text-sm font-medium text-gray-700 mb-1'>
              Customer name <span className='text-red-500'>*</span>
            </label>
            <Input
              value={formData.customerName}
              onChange={(e) => setFormData({ ...formData, customerName: e.target.value })}
              placeholder='Enter customer name'
              required
            />
          </div>
          <div className='col-span-2'>
            <label className='block text-sm font-medium text-gray-700 mb-1'>Delivery address</label>
            <Input
              value={formData.deliveryAddress}
              onChange={(e) => setFormData({ ...formData, deliveryAddress: e.target.value })}
              placeholder='Enter delivery address'
            />
          </div>
          <div className='col-span-2'>
            <label className='block text-sm font-medium text-gray-700 mb-1'>Remarks</label>
            <Input
              value={formData.remarks}
              onChange={(e) => setFormData({ ...formData, remarks: e.target.value })}
              placeholder='Enter remarks'
            />
          </div>
        </div>

        {/* Line items */}
        <div className='border-t border-gray-200 pt-4'>
          <div className='flex items-center justify-between mb-2'>
            <h3 className='text-lg font-medium text-gray-900'>Line items</h3>
            <Button type='button' variant='outline' size='sm' onClick={addLine}>
              <Plus className='w-4 h-4 mr-2' />
              Add item
            </Button>
          </div>

          <div className='space-y-4'>
            {lines.map((line, index) => (
              <div key={index} className='flex gap-2 items-start bg-gray-50 p-3 rounded-lg'>
                <div className='grid grid-cols-12 gap-2 flex-1'>
                  <div className='col-span-3'>
                    <Input
                      placeholder='Item code'
                      value={line.itemCode}
                      onChange={(e) => handleLineChange(index, 'itemCode', e.target.value)}
                      required
                    />
                  </div>
                  <div className='col-span-3'>
                    <Input
                      placeholder='Item name'
                      value={line.itemName}
                      onChange={(e) => handleLineChange(index, 'itemName', e.target.value)}
                      required
                    />
                  </div>
                  <div className='col-span-2'>
                    <Input
                      type='number'
                      placeholder='Qty'
                      value={line.quantity.toString()}
                      onChange={(e) => {
                        let value = e.target.value;
                        if (value.length > 1 && value.startsWith('0')) {
                          value = value.replace(/^0+/, '');
                        }
                        const numValue = Number(value);
                        handleLineChange(index, 'quantity', isNaN(numValue) ? 0 : numValue);
                      }}
                      onFocus={(e) => e.target.select()}
                      min='1'
                      required
                    />
                  </div>
                  <div className='col-span-2'>
                    <Input
                      type='number'
                      placeholder='Unit price'
                      value={line.unitPrice.toString()}
                      onChange={(e) => {
                        let value = e.target.value;
                        if (value.length > 1 && value.startsWith('0')) {
                          value = value.replace(/^0+/, '');
                        }
                        const numValue = Number(value);
                        handleLineChange(index, 'unitPrice', isNaN(numValue) ? 0 : numValue);
                      }}
                      onFocus={(e) => e.target.select()}
                      min='0'
                      required
                    />
                  </div>
                  <div className='col-span-2'>
                    <Input
                      placeholder='Remarks'
                      value={line.remarks}
                      onChange={(e) => handleLineChange(index, 'remarks', e.target.value)}
                    />
                  </div>
                </div>
                {lines.length > 1 && (
                  <button
                    type='button'
                    onClick={() => removeLine(index)}
                    className='mt-2 text-red-500 hover:text-red-700 p-1'
                  >
                    <Trash2 className='w-4 h-4' />
                  </button>
                )}
              </div>
            ))}
          </div>

          <div className='mt-4 flex justify-end text-lg font-bold text-gray-900'>
            Total: {new Intl.NumberFormat('ko-KR', { style: 'currency', currency: 'KRW' }).format(totalAmount)}
          </div>
        </div>

        {/* Action buttons */}
        <div className='flex justify-end gap-2 pt-4'>
          <Button type='button' variant='ghost' onClick={onClose}>
            Cancel
          </Button>
          <Button type='submit' disabled={createMutation.isPending}>
            {createMutation.isPending ? 'Processing...' : 'Create order'}
          </Button>
        </div>
      </form>
    </Modal>
  );
}
