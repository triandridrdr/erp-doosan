import { useEffect, useState } from 'react';

import { Button } from '../../components/ui/Button';
import { Input } from '../../components/ui/Input';
import { Modal } from '../../components/ui/Modal';
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
  const [defaultBomMasterId, setDefaultBomMasterId] = useState('');

  useEffect(() => {
    if (!isOpen) return;

    setProductId(initial?.productId != null ? String(initial.productId) : '');
    setStyleCode(initial?.styleCode ?? '');
    setStyleName(initial?.styleName ?? '');
    setSeason(initial?.season ?? '');
    setDescription(initial?.description ?? '');
    setDefaultBomMasterId(initial?.defaultBomMasterId != null ? String(initial.defaultBomMasterId) : '');
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
            defaultBomMasterId: defaultBomMasterId.trim() ? Number(defaultBomMasterId.trim()) : undefined,
          });
        }}
      >
        <label className='text-sm font-semibold text-gray-700'>Product ID</label>
        <Input placeholder='Product ID' value={productId} onChange={(e) => setProductId(e.target.value)} />
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

        <label className='text-sm font-semibold text-gray-700'>Default BoM Master ID</label>
        <Input
          placeholder='Default BoM Master ID'
          value={defaultBomMasterId}
          onChange={(e) => setDefaultBomMasterId(e.target.value)}
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
