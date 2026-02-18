/**
 * @file features/accounting/JournalEntryCreateModal.tsx
 * @description Modal component to create a new journal entry.
 * Supports dynamically adding/removing debit/credit lines and validates totals.
 */
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Plus, Trash2 } from 'lucide-react';
import React, { useState } from 'react';

import { Button } from '../../components/ui/Button';
import { Input } from '../../components/ui/Input';
import { Modal } from '../../components/ui/Modal';
import { accountingApi, type JournalEntryCreateRequest } from './api';

interface Props {
  isOpen: boolean;
  onClose: () => void;
}

// Initial entry line
const initialLine = {
  accountCode: '',
  accountName: '',
  debit: 0,
  credit: 0,
  description: '',
};

export function JournalEntryCreateModal({ isOpen, onClose }: Props) {
  const queryClient = useQueryClient();
  // Form state: entry date (defaults to today)
  const [entryDate, setEntryDate] = useState(new Date().toISOString().split('T')[0]);
  const [description, setDescription] = useState('');

  // Entry lines state (start with 2 rows for debit/credit)
  const [lines, setLines] = useState([initialLine, initialLine]);

  // Create mutation
  const createMutation = useMutation({
    mutationFn: accountingApi.create,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['journal-entries'] });
      onClose();
      // Reset form
      setEntryDate(new Date().toISOString().split('T')[0]);
      setDescription('');
      setLines([initialLine, initialLine]);
      alert('Journal entry created successfully.');
    },
    onError: (error: Error) => {
      alert(`Failed to create journal entry: ${error.message}`);
    },
  });

  // Line change handler
  const handleLineChange = (index: number, field: string, value: string | number) => {
    const newLines = [...lines];
    newLines[index] = { ...newLines[index], [field]: value };
    setLines(newLines);
  };

  // Add line
  const addLine = () => {
    setLines([...lines, initialLine]);
  };

  // Remove line (keep at least 2 rows)
  const removeLine = (index: number) => {
    if (lines.length > 2) {
      setLines(lines.filter((_, i) => i !== index));
    }
  };

  // Calculate totals
  const totalDebit = lines.reduce((sum, line) => sum + Number(line.debit), 0);
  const totalCredit = lines.reduce((sum, line) => sum + Number(line.credit), 0);

  // Form submit handler
  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();

    // Validate debit/credit totals (double-entry accounting)
    if (totalDebit !== totalCredit) {
      alert('Total debit and total credit do not match.');
      return;
    }
    if (totalDebit === 0) {
      alert('Please enter an amount.');
      return;
    }

    const payload: JournalEntryCreateRequest = {
      entryDate,
      description,
      lines: lines.map((line, index) => ({
        ...line,
        lineNumber: index + 1,
        debit: Number(line.debit),
        credit: Number(line.credit),
      })),
    };

    createMutation.mutate(payload);
  };

  return (
    <Modal isOpen={isOpen} onClose={onClose} title='New Journal Entry'>
      <form onSubmit={handleSubmit} className='space-y-6'>
        {/* Header fields */}
        <div className='grid grid-cols-2 gap-4'>
          <div>
            <label className='block text-sm font-medium text-gray-700 mb-1'>
              Entry date <span className='text-red-500'>*</span>
            </label>
            <Input type='date' value={entryDate} onChange={(e) => setEntryDate(e.target.value)} required />
          </div>
          <div className='col-span-2'>
            <label className='block text-sm font-medium text-gray-700 mb-1'>
              Description <span className='text-red-500'>*</span>
            </label>
            <Input
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder='Enter a description'
              required
            />
          </div>
        </div>

        {/* Entry lines */}
        <div className='border-t border-gray-200 pt-4'>
          <div className='flex items-center justify-between mb-2'>
            <h3 className='text-lg font-medium text-gray-900'>Entry lines</h3>
            <Button type='button' variant='outline' size='sm' onClick={addLine}>
              <Plus className='w-4 h-4 mr-2' />
              Add line
            </Button>
          </div>

          <div className='space-y-4'>
            {lines.map((line, index) => (
              <div key={index} className='flex gap-2 items-start bg-gray-50 p-3 rounded-lg'>
                <div className='grid grid-cols-12 gap-2 flex-1'>
                  <div className='col-span-2'>
                    <Input
                      placeholder='Account code'
                      value={line.accountCode}
                      onChange={(e) => handleLineChange(index, 'accountCode', e.target.value)}
                      required
                    />
                  </div>
                  <div className='col-span-3'>
                    <Input
                      placeholder='Account name'
                      value={line.accountName}
                      onChange={(e) => handleLineChange(index, 'accountName', e.target.value)}
                      required
                    />
                  </div>
                  <div className='col-span-2'>
                    <Input
                      type='number'
                      placeholder='Debit'
                      value={line.debit}
                      onChange={(e) => handleLineChange(index, 'debit', e.target.value)}
                      min='0'
                    />
                  </div>
                  <div className='col-span-2'>
                    <Input
                      type='number'
                      placeholder='Credit'
                      value={line.credit}
                      onChange={(e) => handleLineChange(index, 'credit', e.target.value)}
                      min='0'
                    />
                  </div>
                  <div className='col-span-3'>
                    <Input
                      placeholder='Line description (optional)'
                      value={line.description}
                      onChange={(e) => handleLineChange(index, 'description', e.target.value)}
                    />
                  </div>
                </div>
                {lines.length > 2 && (
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

          {/* Totals */}
          <div className='mt-4 flex justify-between items-center bg-gray-100 p-4 rounded-lg'>
            <div className='text-sm font-medium text-gray-600'>Totals</div>
            <div className={`text-lg font-bold ${totalDebit === totalCredit ? 'text-green-600' : 'text-red-600'}`}>
              Debit: {new Intl.NumberFormat('ko-KR').format(totalDebit)} / Credit: {new Intl.NumberFormat('ko-KR').format(totalCredit)}
            </div>
          </div>
          {totalDebit !== totalCredit && (
            <div className='text-right text-sm text-red-500 mt-1'>
              Difference: {new Intl.NumberFormat('ko-KR').format(Math.abs(totalDebit - totalCredit))}
            </div>
          )}
        </div>

        {/* Action buttons */}
        <div className='flex justify-end gap-2 pt-4'>
          <Button type='button' variant='ghost' onClick={onClose}>
            Cancel
          </Button>
          <Button type='submit' disabled={createMutation.isPending}>
            {createMutation.isPending ? 'Processing...' : 'Create entry'}
          </Button>
        </div>
      </form>
    </Modal>
  );
}
