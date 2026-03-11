import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { useState } from 'react';

import { Button } from '../../components/ui/Button';
import { Modal } from '../../components/ui/Modal';
import { cn } from '../../lib/utils';
import { ocrDraftApi } from './api';

type DraftListItem = {
  id: number;
  sourceFilename?: string;
  soNumber?: string;
  status?: 'DRAFT' | 'APPROVED' | string;
  createdAt?: string;
};

export function OcrBomPage() {
  const navigate = useNavigate();
  const qc = useQueryClient();

  const [isDeleteOpen, setIsDeleteOpen] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<DraftListItem | null>(null);

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['ocr-drafts'],
    queryFn: async () => {
      const res = await ocrDraftApi.list();
      const out = (res?.data as DraftListItem[]) || [];
      return out.filter((x) => (x.status || '').toUpperCase() !== 'DELETED');
    },
  });

  const { mutate: deleteDraft, isPending: isDeletePending } = useMutation({
    mutationFn: (id: number) => ocrDraftApi.delete(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['ocr-drafts'] });
      setIsDeleteOpen(false);
      setDeleteTarget(null);
    },
    onError: () => {
      setIsDeleteOpen(false);
      setDeleteTarget(null);
    },
  });

  const badgeClass = (status?: string) => {
    switch ((status || '').toUpperCase()) {
      case 'APPROVED':
        return 'bg-green-100 text-green-800';
      case 'DRAFT':
        return 'bg-yellow-100 text-yellow-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  return (
    <div className='space-y-6'>
      <div className='flex items-center justify-between'>
        <h1 className='text-2xl font-bold text-gray-900'>BoM Master from OCR</h1>
        <Button variant='outline' onClick={() => refetch()}>
          Refresh
        </Button>
      </div>

      <div className='bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden'>
        <div className='overflow-x-auto'>
          <table className='min-w-full divide-y divide-gray-200'>
            <thead className='bg-gray-50'>
              <tr>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>ID</th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>SO Number</th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>Source File</th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>Created</th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>Status</th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>Actions</th>
              </tr>
            </thead>
            <tbody className='bg-white divide-y divide-gray-200'>
              {isLoading && (
                <tr>
                  <td colSpan={6} className='px-6 py-10 text-center text-gray-500'>
                    Loading...
                  </td>
                </tr>
              )}
              {error && (
                <tr>
                  <td colSpan={6} className='px-6 py-10 text-center text-red-500'>
                    Failed to load OCR drafts.
                  </td>
                </tr>
              )}
              {!isLoading && !error && (data || []).length === 0 && (
                <tr>
                  <td colSpan={6} className='px-6 py-10 text-center text-gray-500'>
                    No OCR drafts saved yet.
                  </td>
                </tr>
              )}

              {(data || []).map((d) => (
                <tr
                  key={d.id}
                  className='hover:bg-gray-50 cursor-pointer transition-colors'
                  onClick={() => navigate(`/ocr-bom-master/${d.id}`)}
                >
                  <td className='px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900'>{d.id}</td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-gray-700'>{d.soNumber || '-'}</td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-gray-500'>{d.sourceFilename || '-'}</td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-gray-500'>{d.createdAt || '-'}</td>
                  <td className='px-6 py-4 whitespace-nowrap'>
                    <span className={cn('px-2.5 py-0.5 rounded-full text-xs font-medium', badgeClass(d.status))}>
                      {d.status || 'UNKNOWN'}
                    </span>
                  </td>
                  <td className='px-6 py-4 whitespace-nowrap'>
                    <div className='flex items-center gap-2'>
                      <Button
                        variant='outline'
                        onClick={(e) => {
                          e.preventDefault();
                          e.stopPropagation();
                          navigate(`/ocr-bom-master/${d.id}`);
                        }}
                      >
                        Edit
                      </Button>
                      <Button
                        variant='danger'
                        onClick={(e) => {
                          e.preventDefault();
                          e.stopPropagation();
                          if (isDeletePending) return;
                          setDeleteTarget(d);
                          setIsDeleteOpen(true);
                        }}
                        disabled={isDeletePending}
                      >
                        Delete
                      </Button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <Modal
        isOpen={isDeleteOpen}
        onClose={() => {
          setIsDeleteOpen(false);
          setDeleteTarget(null);
        }}
        title='Delete draft'
      >
        <div className='space-y-6'>
          <div className='text-sm text-gray-700'>Are you sure you want to delete draft #{deleteTarget?.id}?</div>
          <div className='flex justify-end gap-2'>
            <Button
              variant='ghost'
              onClick={() => {
                setIsDeleteOpen(false);
                setDeleteTarget(null);
              }}
              disabled={isDeletePending}
            >
              Cancel
            </Button>
            <Button
              variant='danger'
              isLoading={isDeletePending}
              disabled={!deleteTarget || isDeletePending}
              onClick={() => {
                if (!deleteTarget) return;
                deleteDraft(deleteTarget.id);
              }}
            >
              Delete
            </Button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
