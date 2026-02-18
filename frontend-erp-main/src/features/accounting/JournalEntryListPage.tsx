/**
 * @file features/accounting/JournalEntryListPage.tsx
 * @description Page component to fetch and display journal entries.
 * Shows entry number, date, description, and debit/credit totals in a table.
 */
import { useQuery } from '@tanstack/react-query';
import { Plus, Search } from 'lucide-react';
import { useState } from 'react';

import { Button } from '../../components/ui/Button';
import { Input } from '../../components/ui/Input';
import { accountingApi } from './api';
import { JournalEntryCreateModal } from './JournalEntryCreateModal';

export function JournalEntryListPage() {
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false); // Create modal state

  // Fetch journal entries
  const { data: journals, isLoading } = useQuery({
    queryKey: ['journal-entries'],
    queryFn: async () => {
      const res = await accountingApi.getAll();
      return res.data;
    },
  });

  return (
    <div className='space-y-6'>
      {/* Header: title and create button */}
      <div className='flex items-center justify-between'>
        <h1 className='text-2xl font-bold text-gray-900'>Journal Entries</h1>
        <Button onClick={() => setIsCreateModalOpen(true)}>
          <Plus className='w-4 h-4 mr-2' />
          New entry
        </Button>
      </div>

      {/* Search filters */}
      <div className='bg-white p-4 rounded-lg shadow-sm border border-gray-200 flex items-center space-x-4'>
        <div className='relative flex-1 max-w-sm'>
          <Search className='absolute left-3 top-2.5 h-4 w-4 text-gray-400' />
          <Input placeholder='Search entry number...' className='pl-9' />
        </div>
      </div>

      {/* Entries table */}
      <div className='bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden'>
        <div className='overflow-x-auto'>
          <table className='min-w-full divide-y divide-gray-200'>
            <thead className='bg-gray-50'>
              <tr>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>
                  Entry No.
                </th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>Date</th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'>Description</th>
                <th className='px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider'>
                  Total debit
                </th>
                <th className='px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider'>
                  Total credit
                </th>
                <th className='px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider'>
                  Status
                </th>
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
              {/* Render rows */}
              {journals?.content?.map((entry) => (
                <tr key={entry.id} className='hover:bg-gray-50 cursor-pointer transition-colors'>
                  <td className='px-6 py-4 whitespace-nowrap text-sm font-medium text-blue-600'>{entry.entryNumber}</td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-gray-500'>{entry.entryDate}</td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-gray-500 truncate max-w-xs'>
                    {entry.description}
                  </td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-gray-900 text-right'>
                    {new Intl.NumberFormat('ko-KR').format(entry.totalDebit)}
                  </td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-gray-900 text-right'>
                    {new Intl.NumberFormat('ko-KR').format(entry.totalCredit)}
                  </td>
                  <td className='px-6 py-4 whitespace-nowrap text-center'>
                    <span className='px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800'>
                      {entry.status}
                    </span>
                  </td>
                </tr>
              ))}
              {/* Empty state */}
              {journals?.content && journals.content.length === 0 && (
                <tr>
                  <td colSpan={6} className='px-6 py-10 text-center text-gray-500'>
                    No entries found.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Create modal */}
      <JournalEntryCreateModal isOpen={isCreateModalOpen} onClose={() => setIsCreateModalOpen(false)} />
    </div>
  );
}
