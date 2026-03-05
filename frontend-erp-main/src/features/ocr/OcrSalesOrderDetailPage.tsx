import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';

import { Button } from '../../components/ui/Button';
import { ocrDraftApi } from './api';
import { getAttachedBomMasterId } from './draftHelpers';

type HeaderRow = { field: string; value: string; editable?: boolean };

type SizeRow = {
  id: string;
  color: string;
  xs: number;
  s: number;
  m: number;
  l: number;
  xl: number;
  total: number;
  editable?: boolean;
};

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
  headerRows: HeaderRow[];
  sizeRows: SizeRow[];
  bomRows: BomRow[];
};

const toIntLoose = (v: unknown) => {
  const s0 = v === null || v === undefined ? '' : String(v);
  const s = s0.trim();
  if (!s) return 0;
  const m = s.replace(/,/g, '').match(/-?\d+/);
  if (!m) return 0;
  const n = Number.parseInt(m[0], 10);
  return Number.isFinite(n) ? n : 0;
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

export function OcrSalesOrderDetailPage() {
  const params = useParams();
  const navigate = useNavigate();
  const qc = useQueryClient();

  const id = Number(params.id);

  const [saveStatus, setSaveStatus] = useState<{ state: 'idle' | 'saving' | 'saved' | 'error'; message?: string }>({
    state: 'idle',
  });

  const { data, isLoading, error } = useQuery({
    queryKey: ['ocr-draft', id],
    enabled: Number.isFinite(id),
    queryFn: async () => {
      const res = await ocrDraftApi.get(id);
      return res?.data as any;
    },
  });

  const status = (data?.status as string) || 'DRAFT';

  const initialDraft = useMemo(() => {
    const d = data?.draft;
    if (!d || typeof d !== 'object') return null;
    const headerRows = Array.isArray((d as any).headerRows) ? ((d as any).headerRows as HeaderRow[]) : [];
    const sizeRows = Array.isArray((d as any).sizeRows) ? ((d as any).sizeRows as SizeRow[]) : [];
    const bomRows = Array.isArray((d as any).bomRows) ? ((d as any).bomRows as BomRow[]) : [];
    const system = (d as any).system;

    const normSizeRows = sizeRows.map((r) => ({
      id: r.id || newRowId(),
      color: r.color || '',
      xs: toIntLoose((r as any).xs),
      s: toIntLoose((r as any).s),
      m: toIntLoose((r as any).m),
      l: toIntLoose((r as any).l),
      xl: toIntLoose((r as any).xl),
      total: toIntLoose((r as any).total),
      editable: true,
    }));

    const fixedSizeRows = normSizeRows.map((r) => ({ ...r, total: r.xs + r.s + r.m + r.l + r.xl }));

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

    const normHeaderRows = headerRows.map((r) => ({ field: r.field, value: r.value || '', editable: true }));

    return { system, headerRows: normHeaderRows, sizeRows: fixedSizeRows, bomRows: normBomRows } as DraftEditorPayload;
  }, [data]);

  const [draft, setDraft] = useState<DraftEditorPayload | null>(null);

  const attachedBomMasterId = getAttachedBomMasterId((data as any)?.draft) ?? getAttachedBomMasterId(draft);

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

  const { mutate: approveDraft, isPending: isApprovePending } = useMutation({
    mutationFn: () => ocrDraftApi.approve(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['ocr-drafts'] });
      qc.invalidateQueries({ queryKey: ['ocr-draft', id] });
    },
  });


  if (!Number.isFinite(id)) {
    return (
      <div className='space-y-4'>
        <div className='text-red-600'>Invalid draft id</div>
        <Button variant='outline' onClick={() => navigate('/ocr-sales-orders')}>
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
        <Button variant='outline' onClick={() => navigate('/ocr-sales-orders')}>
          Back
        </Button>
      </div>
    );
  }

  const onSave = () => {
    if (!draft) return;
    const headerMap = new Map(draft.headerRows.map((r) => [r.field, r.value] as const));
    const soNumber = headerMap.get('SO Number') || data?.soNumber || '';
    saveDraft({
      sourceFilename: data?.sourceFilename,
      soNumber,
      draft,
    });
  };

  return (
    <div className='space-y-6'>
      <div className='flex items-center justify-between'>
        <div className='space-y-1'>
          <div className='text-sm text-gray-500'>
            {attachedBomMasterId !== undefined ? `Attached BoM Master ID: ${attachedBomMasterId}` : `OCR Draft ID: ${id}`}
          </div>
          <h1 className='text-2xl font-bold text-gray-900'>Sales Order from OCR</h1>
          <div className='text-sm text-gray-600'>Status: {status}</div>
        </div>
        <div className='flex items-center gap-2'>
          {saveStatus.state === 'error' && <div className='text-sm text-red-600'>{saveStatus.message}</div>}
          {saveStatus.state === 'saved' && <div className='text-sm text-green-700'>Saved</div>}
          <Button variant='outline' onClick={() => navigate('/ocr-sales-orders')}>
            Back
          </Button>
          <Button onClick={onSave} disabled={isSavePending}>
            {isSavePending ? 'Saving...' : 'Save'}
          </Button>
          <Button
            className='bg-green-600 hover:bg-green-700'
            onClick={() => {
              approveDraft();
            }}
            disabled={
              isApprovePending ||
              String(status).toUpperCase() === 'APPROVED'
            }
          >
            {String(status).toUpperCase() === 'APPROVED' ? 'Approved' : isApprovePending ? 'Approving...' : 'Approve'}
          </Button>
        </div>
      </div>

      <div className='bg-white rounded-lg border border-gray-200 overflow-hidden'>
        <div className='bg-gray-50 px-4 py-3 border-b border-gray-200'>
          <h3 className='font-semibold text-gray-900'>SECTION 1 – SALES ORDER HEADER (DRAFT)</h3>
        </div>
        <div className='p-4 overflow-x-auto'>
          <table className='min-w-full divide-y divide-gray-200'>
            <thead className='bg-gray-50'>
              <tr>
                <th className='px-4 py-2 text-left text-xs font-semibold text-gray-600'>Field</th>
                <th className='px-4 py-2 text-left text-xs font-semibold text-gray-600'>Value</th>
              </tr>
            </thead>
            <tbody className='bg-white divide-y divide-gray-100'>
              {draft.headerRows.map((r) => (
                <tr key={r.field}>
                  <td className='px-4 py-2 text-sm text-gray-700 whitespace-nowrap'>{r.field}</td>
                  <td className='px-4 py-2 text-sm text-gray-900'>
                    <input
                      className='w-full border border-gray-200 rounded px-2 py-1 text-sm'
                      value={r.value}
                      onChange={(e) => {
                        setDraft((cur) => {
                          if (!cur) return cur;
                          return {
                            ...cur,
                            headerRows: cur.headerRows.map((x) => (x.field === r.field ? { ...x, value: e.target.value } : x)),
                          };
                        });
                      }}
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className='bg-white rounded-lg border border-gray-200 overflow-hidden'>
        <div className='bg-gray-50 px-4 py-3 border-b border-gray-200'>
          <div className='flex items-center justify-between gap-4'>
            <h3 className='font-semibold text-gray-900'>SECTION 2 – SALES ORDER DETAIL (SIZE BREAKDOWN)</h3>
            <Button
              className='h-9 px-3 text-sm'
              onClick={() => {
                setDraft((cur) => {
                  if (!cur) return cur;
                  return {
                    ...cur,
                    sizeRows: [
                      ...cur.sizeRows,
                      { id: newRowId(), color: '', xs: 0, s: 0, m: 0, l: 0, xl: 0, total: 0, editable: true },
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
                <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Color</th>
                <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>XS</th>
                <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>S</th>
                <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>M</th>
                <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>L</th>
                <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>XL</th>
                <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Total</th>
                <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Actions</th>
              </tr>
            </thead>
            <tbody className='bg-white divide-y divide-gray-100'>
              {draft.sizeRows.map((r, idx) => (
                <tr key={r.id}>
                  <td className='px-3 py-2 text-sm text-gray-700'>
                    <input
                      className='w-full border border-gray-200 rounded px-2 py-1 text-sm'
                      value={r.color}
                      onChange={(e) => {
                        setDraft((cur) => {
                          if (!cur) return cur;
                          const next = [...cur.sizeRows];
                          next[idx] = { ...next[idx], color: e.target.value };
                          return { ...cur, sizeRows: next };
                        });
                      }}
                    />
                  </td>
                  {(['xs', 's', 'm', 'l', 'xl'] as const).map((k) => (
                    <td key={k} className='px-3 py-2 text-sm text-gray-900'>
                      <input
                        className='w-24 border border-gray-200 rounded px-2 py-1 text-sm'
                        value={String(r[k])}
                        onChange={(e) => {
                          const v = toIntLoose(e.target.value);
                          setDraft((cur) => {
                            if (!cur) return cur;
                            const next = [...cur.sizeRows];
                            const row = { ...next[idx], [k]: v } as SizeRow;
                            row.total = row.xs + row.s + row.m + row.l + row.xl;
                            next[idx] = row;
                            return { ...cur, sizeRows: next };
                          });
                        }}
                      />
                    </td>
                  ))}
                  <td className='px-3 py-2 text-sm text-gray-900 font-semibold'>{r.total}</td>
                  <td className='px-3 py-2 text-sm text-gray-700'>
                    <Button
                      className='h-8 px-3 text-sm bg-red-600 hover:bg-red-700'
                      onClick={() => {
                        setDraft((cur) => {
                          if (!cur) return cur;
                          return { ...cur, sizeRows: cur.sizeRows.filter((x) => x.id !== r.id) };
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

      <div className='bg-white rounded-lg border border-gray-200 overflow-hidden'>
        <div className='bg-gray-50 px-4 py-3 border-b border-gray-200'>
          <div className='flex items-center justify-between gap-4'>
            <h3 className='font-semibold text-gray-900'>SECTION 3 – BILL OF MATERIALS (BOM DRAFT)</h3>
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
