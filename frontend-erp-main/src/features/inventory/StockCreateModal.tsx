/**
 * @file features/inventory/StockCreateModal.tsx
 * @description Modal component to create a new stock item.
 * Accepts item/warehouse information and initial quantity.
 */
import { useMutation, useQueryClient } from '@tanstack/react-query';
import React, { useState } from 'react';

import { Button } from '../../components/ui/Button';
import { Input } from '../../components/ui/Input';
import { Modal } from '../../components/ui/Modal';
import { inventoryApi, type StockCreateRequest } from './api';

interface Props {
  isOpen: boolean;
  onClose: () => void;
}

export function StockCreateModal({ isOpen, onClose }: Props) {
  const queryClient = useQueryClient();

  // Form state
  const [formData, setFormData] = useState<Partial<StockCreateRequest>>({
    itemCode: '',
    itemName: '',
    warehouseCode: '',
    warehouseName: '',
    quantity: 0,
    unit: 'EA',
    unitPrice: 0,
  });

  // Create mutation
  const createMutation = useMutation({
    mutationFn: inventoryApi.create,
    onSuccess: () => {
      // Refresh list and close modal on success
      queryClient.invalidateQueries({ queryKey: ['stocks'] });
      onClose();
      // Reset form
      setFormData({
        itemCode: '',
        itemName: '',
        warehouseCode: '',
        warehouseName: '',
        quantity: 0,
        unit: 'EA',
        unitPrice: 0,
      });
      alert('Stock created successfully.');
    },
    onError: (error: Error) => {
      alert(`Failed to create stock: ${error.message}`);
    },
  });

  // Form submit handler
  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    // Validate required fields
    if (!formData.itemCode || !formData.itemName || !formData.warehouseCode) {
      alert('Please fill in the required fields.');
      return;
    }

    createMutation.mutate(formData as StockCreateRequest);
  };

  return (
    <Modal isOpen={isOpen} onClose={onClose} title='New Stock'>
      <form onSubmit={handleSubmit} className='space-y-4'>
        <div className='grid grid-cols-2 gap-4'>
          <div>
            <label className='block text-sm font-medium text-gray-700 mb-1'>
              Item code <span className='text-red-500'>*</span>
            </label>
            <Input
              value={formData.itemCode}
              onChange={(e) => setFormData({ ...formData, itemCode: e.target.value })}
              placeholder='ITEM-001'
              required
            />
          </div>
          <div>
            <label className='block text-sm font-medium text-gray-700 mb-1'>
              Item name <span className='text-red-500'>*</span>
            </label>
            <Input
              value={formData.itemName}
              onChange={(e) => setFormData({ ...formData, itemName: e.target.value })}
              placeholder='Enter item name'
              required
            />
          </div>
          <div>
            <label className='block text-sm font-medium text-gray-700 mb-1'>
              Warehouse code <span className='text-red-500'>*</span>
            </label>
            <Input
              value={formData.warehouseCode}
              onChange={(e) => setFormData({ ...formData, warehouseCode: e.target.value })}
              placeholder='WH-001'
              required
            />
          </div>
          <div>
            <label className='block text-sm font-medium text-gray-700 mb-1'>
              Warehouse name <span className='text-red-500'>*</span>
            </label>
            <Input
              value={formData.warehouseName}
              onChange={(e) => setFormData({ ...formData, warehouseName: e.target.value })}
              placeholder='Enter warehouse name'
              required
            />
          </div>
          <div>
            <label className='block text-sm font-medium text-gray-700 mb-1'>
              Quantity <span className='text-red-500'>*</span>
            </label>
            <Input
              type='number'
              value={(formData.quantity ?? 0).toString()}
              onChange={(e) => {
                let value = e.target.value;
                // If input starts with 0 and length >= 2 (e.g. "01"), remove leading zeros
                if (value.length > 1 && value.startsWith('0')) {
                  value = value.replace(/^0+/, '');
                }
                const numValue = Number(value);
                setFormData({ ...formData, quantity: isNaN(numValue) ? 0 : numValue });
              }}
              onFocus={(e) => e.target.select()}
              min='0'
              required
            />
          </div>
          <div>
            <label className='block text-sm font-medium text-gray-700 mb-1'>
              Unit <span className='text-red-500'>*</span>
            </label>
            <Input
              value={formData.unit}
              onChange={(e) => setFormData({ ...formData, unit: e.target.value })}
              placeholder='EA'
              required
            />
          </div>
          <div className='col-span-2'>
            <label className='block text-sm font-medium text-gray-700 mb-1'>
              Unit price <span className='text-red-500'>*</span>
            </label>
            <Input
              type='number'
              value={(formData.unitPrice ?? 0).toString()}
              onChange={(e) => {
                let value = e.target.value;
                // If input starts with 0 and length >= 2 (e.g. "01"), remove leading zeros
                if (value.length > 1 && value.startsWith('0')) {
                  value = value.replace(/^0+/, '');
                }
                const numValue = Number(value);
                setFormData({ ...formData, unitPrice: isNaN(numValue) ? 0 : numValue });
              }}
              onFocus={(e) => e.target.select()}
              min='0'
              required
            />
          </div>
        </div>

        <div className='flex justify-end gap-2 pt-4'>
          <Button type='button' variant='ghost' onClick={onClose}>
            Cancel
          </Button>
          <Button type='submit' disabled={createMutation.isPending}>
            {createMutation.isPending ? 'Processing...' : 'Create stock'}
          </Button>
        </div>
      </form>
    </Modal>
  );
}
