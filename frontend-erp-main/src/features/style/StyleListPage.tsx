import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Pencil, Plus, Search, Trash2 } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';

import { Button } from '../../components/ui/Button';
import { Input } from '../../components/ui/Input';
import { Modal } from '../../components/ui/Modal';
import { StyleFormModal } from './StyleFormModal';
import type { Style, StyleRequest } from './api';
import { styleApi } from './api';

export function StyleListPage() {
  const queryClient = useQueryClient();

  const [page, setPage] = useState(0);
  const [size] = useState(20);
  const [search, setSearch] = useState('');

  const [isFormOpen, setIsFormOpen] = useState(false);
  const [editing, setEditing] = useState<Style | null>(null);

  const [isSuccessOpen, setIsSuccessOpen] = useState(false);
  const [successMessage, setSuccessMessage] = useState('');

  const [isErrorOpen, setIsErrorOpen] = useState(false);
  const [errorMessage, setErrorMessage] = useState('');

  const [isDeleteOpen, setIsDeleteOpen] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<Style | null>(null);

  const {
    data: stylePage,
    isLoading,
    error,
  } = useQuery({
    queryKey: ['styles', { page, size, search }],
    queryFn: async () => {
      const res = await styleApi.list({ page, size, search: search.trim() ? search.trim() : undefined });
      return res.data;
    },
  });

  const styles = useMemo(() => stylePage?.content ?? [], [stylePage?.content]);

  useEffect(() => {
    if ((stylePage?.totalPages ?? 0) > 0 && page > (stylePage?.totalPages ?? 1) - 1) {
      setPage(Math.max(0, (stylePage?.totalPages ?? 1) - 1));
    }
  }, [page, stylePage?.totalPages]);

  const createMutation = useMutation({
    mutationFn: async (data: StyleRequest) => {
      return styleApi.create(data);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['styles'] });
      setIsFormOpen(false);
      setEditing(null);
      setSuccessMessage('Style berhasil disimpan.');
      setIsSuccessOpen(true);
    },
    onError: (err: any) => {
      const msg = err?.response?.data?.message || err?.message || 'Gagal menyimpan style.';
      setErrorMessage(String(msg));
      setIsErrorOpen(true);
    },
  });

  const updateMutation = useMutation({
    mutationFn: async ({ id, data }: { id: number; data: StyleRequest }) => {
      return styleApi.update(id, data);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['styles'] });
      setIsFormOpen(false);
      setEditing(null);
      setSuccessMessage('Style berhasil disimpan.');
      setIsSuccessOpen(true);
    },
    onError: (err: any) => {
      const msg = err?.response?.data?.message || err?.message || 'Gagal menyimpan style.';
      setErrorMessage(String(msg));
      setIsErrorOpen(true);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: async (id: number) => {
      return styleApi.delete(id);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['styles'] });
      setIsDeleteOpen(false);
      setDeleteTarget(null);
    },
  });

  return (
    <div className='space-y-6'>
      <div className='flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between'>
        <h1 className='text-2xl font-bold text-gray-900'>Styles</h1>
        <Button
          className='w-full sm:w-auto'
          onClick={() => {
            setEditing(null);
            setIsFormOpen(true);
          }}
        >
          <Plus className='w-4 h-4 mr-2' />
          New style
        </Button>
      </div>

      <div className='bg-white p-4 rounded-lg shadow-sm border border-gray-200 flex flex-col sm:flex-row sm:items-center sm:space-x-4 gap-3 sm:gap-0'>
        <div className='relative flex-1 w-full sm:max-w-sm'>
          <Search className='absolute left-3 top-2.5 h-4 w-4 text-gray-400' />
          <Input
            placeholder='Search style code, name, season...'
            className='pl-9'
            value={search}
            onChange={(e) => {
              setSearch(e.target.value);
              setPage(0);
            }}
          />
        </div>
      </div>

      <div className='bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden'>
        <div className='overflow-x-auto'>
          <table className='min-w-full divide-y divide-gray-200'>
            <thead className='bg-gray-50'>
              <tr>
                <th className='px-4 sm:px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>Style code</th>
                <th className='px-4 sm:px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>Style name</th>
                <th className='px-4 sm:px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>Season</th>
                <th className='px-4 sm:px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>Product ID</th>
                <th className='px-4 sm:px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider'>Actions</th>
              </tr>
            </thead>
            <tbody className='bg-white divide-y divide-gray-200'>
              {isLoading && (
                <tr>
                  <td colSpan={5} className='px-6 py-10 text-center text-gray-500'>
                    Loading...
                  </td>
                </tr>
              )}

              {error && (
                <tr>
                  <td colSpan={5} className='px-6 py-10 text-center text-red-500'>
                    An error occurred while loading data.
                  </td>
                </tr>
              )}

              {!isLoading && !error && styles.length === 0 && (
                <tr>
                  <td colSpan={5} className='px-6 py-10 text-center text-gray-500'>
                    No styles found.
                  </td>
                </tr>
              )}

              {styles.map((s) => (
                <tr key={s.id} className='hover:bg-gray-50 transition-colors'>
                  <td className='px-4 sm:px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900'>{s.styleCode}</td>
                  <td className='px-4 sm:px-6 py-4 whitespace-nowrap text-sm text-gray-700'>{s.styleName}</td>
                  <td className='px-4 sm:px-6 py-4 whitespace-nowrap text-sm text-gray-700'>{s.season ?? '-'}</td>
                  <td className='px-4 sm:px-6 py-4 whitespace-nowrap text-sm text-gray-700'>{s.productId ?? '-'}</td>
                  <td className='px-4 sm:px-6 py-4 whitespace-nowrap text-right'>
                    <div className='flex justify-end gap-2'>
                      <Button
                        variant='outline'
                        onClick={() => {
                          setEditing(s);
                          setIsFormOpen(true);
                        }}
                      >
                        <Pencil className='w-4 h-4 mr-2' />
                        Edit
                      </Button>
                      <Button
                        variant='danger'
                        onClick={() => {
                          setDeleteTarget(s);
                          setIsDeleteOpen(true);
                        }}
                      >
                        <Trash2 className='w-4 h-4 mr-2' />
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

      <div className='flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between'>
        <div className='text-sm text-gray-600'>
          Page {((stylePage?.page ?? 0) + 1).toString()} of {(stylePage?.totalPages ?? 1).toString()} (Total {(stylePage?.totalElements ?? 0).toString()})
        </div>
        <div className='flex items-center gap-2 justify-end'>
          <Button
            variant='outline'
            disabled={isLoading || (stylePage?.first ?? true)}
            onClick={() => setPage((p) => Math.max(0, p - 1))}
          >
            Prev
          </Button>
          <Button
            variant='outline'
            disabled={isLoading || (stylePage?.last ?? true)}
            onClick={() => setPage((p) => p + 1)}
          >
            Next
          </Button>
        </div>
      </div>

      <StyleFormModal
        isOpen={isFormOpen}
        onClose={() => {
          setIsFormOpen(false);
          setEditing(null);
        }}
        initial={editing}
        isSaving={createMutation.isPending || updateMutation.isPending}
        onSubmit={(data) => {
          if (editing) {
            updateMutation.mutate({ id: editing.id, data });
            return;
          }
          createMutation.mutate(data);
        }}
      />

      <Modal
        isOpen={isDeleteOpen}
        onClose={() => {
          setIsDeleteOpen(false);
          setDeleteTarget(null);
        }}
        title='Delete style'
      >
        <div className='space-y-6'>
          <div className='text-sm text-gray-700'>
            Are you sure you want to delete style {deleteTarget?.styleCode}?
          </div>
          <div className='flex justify-end gap-2'>
            <Button
              variant='ghost'
              onClick={() => {
                setIsDeleteOpen(false);
                setDeleteTarget(null);
              }}
              disabled={deleteMutation.isPending}
            >
              Cancel
            </Button>
            <Button
              variant='danger'
              isLoading={deleteMutation.isPending}
              disabled={!deleteTarget || deleteMutation.isPending}
              onClick={() => {
                if (!deleteTarget) return;
                deleteMutation.mutate(deleteTarget.id);
              }}
            >
              Delete
            </Button>
          </div>
        </div>
      </Modal>

      <Modal
        isOpen={isSuccessOpen}
        onClose={() => {
          setIsSuccessOpen(false);
          setSuccessMessage('');
        }}
        title='Success'
      >
        <div className='space-y-6'>
          <div className='text-sm text-gray-700'>{successMessage}</div>
          <div className='flex justify-end gap-2'>
            <Button
              onClick={() => {
                setIsSuccessOpen(false);
                setSuccessMessage('');
              }}
            >
              OK
            </Button>
          </div>
        </div>
      </Modal>

      <Modal
        isOpen={isErrorOpen}
        onClose={() => {
          setIsErrorOpen(false);
          setErrorMessage('');
        }}
        title='Error'
      >
        <div className='space-y-6'>
          <div className='text-sm text-red-600'>{errorMessage}</div>
          <div className='flex justify-end gap-2'>
            <Button
              onClick={() => {
                setIsErrorOpen(false);
                setErrorMessage('');
              }}
            >
              OK
            </Button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
