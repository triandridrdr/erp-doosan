import { useEffect, useState } from 'react';
import { useQuery } from '@tanstack/react-query';

import { Button } from '../../components/ui/Button';
import { Input } from '../../components/ui/Input';
import { Modal } from '../../components/ui/Modal';
import { productApi } from '../product/api';
import type { Style, StyleRequest } from './api';

interface StyleFormModalProps {
  isOpen: boolean;
  onClose: () => void;
  initial?: Style | null;
  isSaving: boolean;
  onSubmit: (data: StyleRequest) => void;
}

export function StyleFormModal({ isOpen, onClose, initial, isSaving, onSubmit }: StyleFormModalProps) {
  const [productId, setProductId] = useState<string>('');
  const [styleCode, setStyleCode] = useState('');
  const [styleName, setStyleName] = useState('');
  const [season, setSeason] = useState('');
  const [description, setDescription] = useState('');

  const { data: productsRes } = useQuery({
    queryKey: ['products'],
    queryFn: async () => {
      return productApi.list();
    },
    enabled: isOpen,
    staleTime: 60 * 1000,
  });

  const products = productsRes?.data ?? [];

  useEffect(() => {
    if (!isOpen) return;

    setProductId(initial?.productId != null ? String(initial.productId) : '');
    setStyleCode(initial?.styleCode ?? '');
    setStyleName(initial?.styleName ?? '');
    setSeason(initial?.season ?? '');
    setDescription(initial?.description ?? '');
  }, [isOpen, initial]);

  return (
    <Modal isOpen={isOpen} onClose={onClose} title={initial ? 'Edit style' : 'New style'}>
      <form
        className='space-y-4'
        onSubmit={(e) => {
          e.preventDefault();
          onSubmit({
            productId: productId.trim() ? productId.trim() : undefined,
            styleCode: styleCode.trim(),
            styleName: styleName.trim(),
            season: season.trim() ? season.trim() : undefined,
            description: description.trim() ? description.trim() : undefined,
          });
        }}
      >
        <div className='w-full space-y-2'>
          <label className='text-sm font-semibold text-gray-700'>Product</label>
          <select
            className='flex h-12 w-full rounded-xl border border-gray-200 bg-gray-50/50 px-3 py-2 text-sm text-gray-900 focus:bg-white focus:outline-none focus:ring-2 focus:ring-primary/20 focus:border-primary transition-all duration-200'
            value={productId}
            onChange={(e) => setProductId(e.target.value)}
          >
            <option value=''>Select product</option>
            {products.map((p) => (
              <option key={p.id} value={String(p.id)}>
                {p.productName ? `${p.productName} (ID: ${p.id})` : `ID: ${p.id}`}
              </option>
            ))}
          </select>
        </div>
        <label className='text-sm font-semibold text-gray-700'>Style Code </label>
        <Input
          placeholder='Style code'
          value={styleCode}
          onChange={(e) => setStyleCode(e.target.value)}
          required
        />
        <label className='text-sm font-semibold text-gray-700'>Style Name </label>
        <Input
          placeholder='Style name'
          value={styleName}
          onChange={(e) => setStyleName(e.target.value)}
          required
        />
        <label className='text-sm font-semibold text-gray-700'>Season </label>
        <Input
          placeholder='Season'
          value={season}
          onChange={(e) => setSeason(e.target.value)}
        />
         <label className='text-sm font-semibold text-gray-700'>Description </label>
        <Input
          placeholder='Description'
          value={description}
          onChange={(e) => setDescription(e.target.value)}
        />

        <div className='flex justify-end gap-2 pt-2'>
          <Button type='button' variant='ghost' onClick={onClose} disabled={isSaving}>
            Cancel
          </Button>
          <Button type='submit' isLoading={isSaving} disabled={isSaving || !styleCode.trim() || !styleName.trim()}>
            Save
          </Button>
        </div>
      </form>
    </Modal>
  );
}
