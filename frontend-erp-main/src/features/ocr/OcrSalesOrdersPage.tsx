import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';

import { Button } from '../../components/ui/Button';
import { cn } from '../../lib/utils';
import { ocrDraftApi } from './api';

type DraftListItem = {
  id: number;
  sourceFilename?: string;
  soNumber?: string;
  status?: 'DRAFT' | 'APPROVED' | string;
  createdAt?: string;
};

export function OcrSalesOrdersPage() {
  const navigate = useNavigate();
  const qc = useQueryClient();

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
    },
  });

  const badgeClass = (status?: string) => {
    switch ((status || '').toUpperCase()) {
      case 'APPROVED':
        return 'bg-green-100 text-green-800';
      case 'DRAFT':
      case 'DELETED':
        return 'bg-yellow-100 text-yellow-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  return (
    <div className='space-y-6'>
      <div className='flex items-center justify-between'>
        <h1 className='text-2xl font-bold text-gray-900'>Sales Order from OCR</h1>
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
                  onClick={() => navigate(`/ocr-sales-orders/${d.id}`)}
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
                        className='h-8 px-3 text-sm'
                        onClick={(e) => {
                          e.preventDefault();
                          e.stopPropagation();
                          navigate(`/ocr-sales-orders/${d.id}`);
                        }}
                      >
                        Edit
                      </Button>
                      <Button
                        className='h-8 px-3 text-sm bg-red-600 hover:bg-red-700'
                        onClick={(e) => {
                          e.preventDefault();
                          e.stopPropagation();
                          if (isDeletePending) return;
                          const ok = window.confirm(`Delete draft #${d.id}?`);
                          if (!ok) return;
                          deleteDraft(d.id);
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
    </div>
  );
}
