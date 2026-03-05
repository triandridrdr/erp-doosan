import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';

import { Button } from '../../components/ui/Button';
import { ocrDraftApi } from './api';
import { getAttachedBomMasterId } from './draftHelpers';

type BomRow = {
  id: string;
  component: string;
  category: string;
  composition: string;
  uom: string;
  consumptionPerUnit: string;
  wastePercent: string;
  editable?: boolean;
};

type DraftEditorPayload = {
  system?: any;
  headerRows?: any[];
  sizeRows?: any[];
  bomRows: BomRow[];
};

const newRowId = () => {
  try {
    const c = (globalThis as any)?.crypto;
    if (c && typeof c.randomUUID === 'function') return c.randomUUID();
  } catch {
    // ignore
  }
  return `${Date.now()}_${Math.random().toString(16).slice(2)}`;
};

export function OcrBomDetailPage() {
  const params = useParams();
  const navigate = useNavigate();
  const qc = useQueryClient();

  const id = Number(params.id);

  const [saveStatus, setSaveStatus] = useState<{ state: 'idle' | 'saving' | 'saved' | 'error'; message?: string }>({
    state: 'idle',
  });

  const [draft, setDraft] = useState<DraftEditorPayload | null>(null);

  const { data, isLoading, error } = useQuery({
    queryKey: ['ocr-draft', id],
    enabled: Number.isFinite(id),
    queryFn: async () => {
      const res = await ocrDraftApi.get(id);
      return res?.data as any;
    },
  });

  const status = (data?.status as string) || 'DRAFT';

  const currentBomMasterId = getAttachedBomMasterId((data as any)?.draft) ?? getAttachedBomMasterId(draft);

  const styleCode = useMemo(() => {
    const rows = ((draft as any)?.headerRows || (data as any)?.draft?.headerRows) as any[] | undefined;
    if (!Array.isArray(rows)) return '';
    const hit = rows.find((r) => String(r?.field || '').trim().toLowerCase() === 'style code');
    const v = hit?.value;
    return v === null || v === undefined ? '' : String(v);
  }, [draft, data]);

  const initialDraft = useMemo(() => {
    const d = data?.draft;
    if (!d || typeof d !== 'object') return null;

    const bomRows = Array.isArray((d as any).bomRows) ? ((d as any).bomRows as BomRow[]) : [];

    const normBomRows = bomRows.map((r) => ({
      id: r.id || newRowId(),
      component: r.component || '',
      category: r.category || '',
      composition: r.composition || '',
      uom: r.uom || '',
      consumptionPerUnit: r.consumptionPerUnit || '',
      wastePercent: r.wastePercent || '',
      editable: true,
    }));

    return {
      system: (d as any).system,
      headerRows: (d as any).headerRows,
      sizeRows: (d as any).sizeRows,
      bomRows: normBomRows,
    } as DraftEditorPayload;
  }, [data]);

  if (draft === null && initialDraft !== null) {
    setDraft(initialDraft);
  }

  const { mutate: saveDraft, isPending: isSavePending } = useMutation({
    mutationFn: (req: any) => ocrDraftApi.update(id, req),
    onMutate: () => setSaveStatus({ state: 'saving' }),
    onSuccess: () => {
      setSaveStatus({ state: 'saved' });
      qc.invalidateQueries({ queryKey: ['ocr-drafts'] });
      qc.invalidateQueries({ queryKey: ['ocr-draft', id] });
    },
    onError: (err: any) => {
      const msg = err?.response?.data?.message || err?.message || 'Save failed';
      setSaveStatus({ state: 'error', message: String(msg) });
    },
  });

  if (!Number.isFinite(id)) {
    return (
      <div className='space-y-4'>
        <div className='text-red-600'>Invalid draft id</div>
        <Button variant='outline' onClick={() => navigate('/ocr-bom-master')}>
          Back
        </Button>
      </div>
    );
  }

  if (isLoading || draft === null) {
    return <div className='text-gray-500'>Loading...</div>;
  }

  if (error) {
    return (
      <div className='space-y-4'>
        <div className='text-red-600'>Failed to load draft.</div>
        <Button variant='outline' onClick={() => navigate('/ocr-bom-master')}>
          Back
        </Button>
      </div>
    );
  }

  const onSave = () => {
    if (!draft) return;
    saveDraft({
      sourceFilename: data?.sourceFilename,
      soNumber: data?.soNumber,
      draft,
    });
  };

  return (
    <div className='space-y-6'>
      <div className='flex items-center justify-between'>
        <div className='space-y-1'>
          <div className='text-sm text-gray-500'>
            {currentBomMasterId !== undefined ? `Attached BoM Master ID: ${currentBomMasterId}` : `OCR Draft ID: ${id}`}
          </div>
          <h1 className='text-2xl font-bold text-gray-900'>BoM Master from OCR</h1>
          <div className='text-sm text-gray-600'>Status: {status}</div>
          <div className='text-sm text-gray-600'>Style Code: {styleCode || '-'}</div>
        </div>
        <div className='flex items-center gap-2'>
          {saveStatus.state === 'error' && <div className='text-sm text-red-600'>{saveStatus.message}</div>}
          {saveStatus.state === 'saved' && <div className='text-sm text-green-700'>Saved</div>}
          <Button variant='outline' onClick={() => navigate('/ocr-bom-master')}>
            Back
          </Button>
          <Button onClick={onSave} disabled={isSavePending}>
            {isSavePending ? 'Saving...' : 'Save'}
          </Button>
        </div>
      </div>

      <div className='bg-white rounded-lg border border-gray-200 overflow-hidden'>
        <div className='bg-gray-50 px-4 py-3 border-b border-gray-200'>
          <div className='flex items-center justify-between gap-4'>
            <h3 className='font-semibold text-gray-900'>BILL OF MATERIALS (BOM DRAFT)</h3>
            <Button
              className='h-9 px-3 text-sm'
              onClick={() => {
                setDraft((cur) => {
                  if (!cur) return cur;
                  return {
                    ...cur,
                    bomRows: [
                      ...cur.bomRows,
                      {
                        id: newRowId(),
                        component: '',
                        category: '',
                        composition: '',
                        uom: '',
                        consumptionPerUnit: '',
                        wastePercent: '',
                        editable: true,
                      },
                    ],
                  };
                });
              }}
            >
              Add row
            </Button>
          </div>
        </div>
        <div className='p-4 overflow-x-auto'>
          <table className='min-w-full divide-y divide-gray-200'>
            <thead className='bg-gray-50'>
              <tr>
                <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Component</th>
                <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Category</th>
                <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Composition</th>
                <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>UOM</th>
                <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Consumption per Unit</th>
                <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Waste %</th>
                <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Actions</th>
              </tr>
            </thead>
            <tbody className='bg-white divide-y divide-gray-100'>
              {draft.bomRows.length === 0 && (
                <tr>
                  <td className='px-3 py-3 text-sm text-gray-500 italic' colSpan={7}>
                    No BoM rows.
                  </td>
                </tr>
              )}
              {draft.bomRows.map((r, idx) => (
                <tr key={r.id}>
                  <td className='px-3 py-2 text-sm text-gray-700'>
                    <input
                      className='w-56 border border-gray-200 rounded px-2 py-1 text-sm'
                      value={r.component}
                      onChange={(e) => {
                        setDraft((cur) => {
                          if (!cur) return cur;
                          const next = [...cur.bomRows];
                          next[idx] = { ...next[idx], component: e.target.value };
                          return { ...cur, bomRows: next };
                        });
                      }}
                    />
                  </td>
                  <td className='px-3 py-2 text-sm text-gray-700'>
                    <input
                      className='w-40 border border-gray-200 rounded px-2 py-1 text-sm'
                      value={r.category}
                      onChange={(e) => {
                        setDraft((cur) => {
                          if (!cur) return cur;
                          const next = [...cur.bomRows];
                          next[idx] = { ...next[idx], category: e.target.value };
                          return { ...cur, bomRows: next };
                        });
                      }}
                    />
                  </td>
                  <td className='px-3 py-2 text-sm text-gray-900'>
                    <textarea
                      className='w-72 border border-gray-200 rounded px-2 py-1 text-sm'
                      rows={2}
                      value={r.composition}
                      onChange={(e) => {
                        setDraft((cur) => {
                          if (!cur) return cur;
                          const next = [...cur.bomRows];
                          next[idx] = { ...next[idx], composition: e.target.value };
                          return { ...cur, bomRows: next };
                        });
                      }}
                    />
                  </td>
                  <td className='px-3 py-2 text-sm text-gray-700'>
                    <input
                      className='w-28 border border-gray-200 rounded px-2 py-1 text-sm'
                      value={r.uom}
                      onChange={(e) => {
                        setDraft((cur) => {
                          if (!cur) return cur;
                          const next = [...cur.bomRows];
                          next[idx] = { ...next[idx], uom: e.target.value };
                          return { ...cur, bomRows: next };
                        });
                      }}
                    />
                  </td>
                  <td className='px-3 py-2 text-sm text-gray-900'>
                    <input
                      className='w-44 border border-gray-200 rounded px-2 py-1 text-sm'
                      value={r.consumptionPerUnit}
                      onChange={(e) => {
                        setDraft((cur) => {
                          if (!cur) return cur;
                          const next = [...cur.bomRows];
                          next[idx] = { ...next[idx], consumptionPerUnit: e.target.value };
                          return { ...cur, bomRows: next };
                        });
                      }}
                    />
                  </td>
                  <td className='px-3 py-2 text-sm text-gray-900'>
                    <input
                      className='w-28 border border-gray-200 rounded px-2 py-1 text-sm'
                      value={r.wastePercent}
                      onChange={(e) => {
                        setDraft((cur) => {
                          if (!cur) return cur;
                          const next = [...cur.bomRows];
                          next[idx] = { ...next[idx], wastePercent: e.target.value };
                          return { ...cur, bomRows: next };
                        });
                      }}
                    />
                  </td>
                  <td className='px-3 py-2 text-sm text-gray-700'>
                    <Button
                      className='h-8 px-3 text-sm bg-red-600 hover:bg-red-700'
                      onClick={() => {
                        setDraft((cur) => {
                          if (!cur) return cur;
                          return { ...cur, bomRows: cur.bomRows.filter((x) => x.id !== r.id) };
                        });
                      }}
                    >
                      Delete
                    </Button>
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
