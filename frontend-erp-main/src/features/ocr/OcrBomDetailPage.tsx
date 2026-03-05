import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';

import { Button } from '../../components/ui/Button';
import { bomMasterApi, ocrDraftApi } from './api';

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

type BomMasterListItem = {
  id: number;
  styleNo: string;
  article: string;
  revision?: number;
  status?: string;
};

const getHeaderValue = (headerRows: any[] | undefined, fieldName: string) => {
  const rows = Array.isArray(headerRows) ? headerRows : [];
  const hit = rows.find((r) => String(r?.field || '').trim() === fieldName);
  return hit?.value ? String(hit.value) : '';
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

  const [manualStyleNo, setManualStyleNo] = useState('');
  const [manualArticle, setManualArticle] = useState('');

  const [isAttachOpen, setIsAttachOpen] = useState(false);
  const [attachStyleNo, setAttachStyleNo] = useState('');
  const [attachArticle, setAttachArticle] = useState('');

  const [draft, setDraft] = useState<DraftEditorPayload | null>(null);

  const { mutate: saveAsBomMaster, isPending: isSaveAsBomPending } = useMutation({
    mutationFn: async () => {
      const styleNo = getHeaderValue(draft?.headerRows, 'Style No') || manualStyleNo;
      const article = getHeaderValue(draft?.headerRows, 'Article') || manualArticle;
      if (!styleNo.trim() || !article.trim()) {
        throw new Error('Style No and Article are required');
      }

      const lines = (draft?.bomRows || []).map((r, idx) => ({
        lineNo: idx + 1,
        component: r.component,
        category: r.category,
        composition: r.composition,
        uom: r.uom,
        consumptionPerUnit: r.consumptionPerUnit,
        wastePercent: r.wastePercent,
      }));

      const created = await bomMasterApi.create({ styleNo, article, lines });
      const bomId = created?.data?.id;
      if (!bomId) {
        throw new Error('Failed to create BoM master');
      }

      const nextDraft = {
        ...(draft as any),
        system: {
          ...((draft as any)?.system || {}),
          bomMasterId: bomId,
        },
      };

      await ocrDraftApi.update(id, {
        sourceFilename: data?.sourceFilename,
        soNumber: data?.soNumber,
        draft: nextDraft,
      });

      return bomId as number;
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['ocr-drafts'] });
      qc.invalidateQueries({ queryKey: ['ocr-draft', id] });
    },
    onError: (err: any) => {
      const msg = err?.response?.data?.message || err?.message || 'Save as BoM master failed';
      setSaveStatus({ state: 'error', message: String(msg) });
    },
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

  const currentBomMasterId = (data as any)?.draft?.system?.bomMasterId ?? (draft as any)?.system?.bomMasterId;

  const { data: bomSearchData, isFetching: isBomSearching, refetch: refetchBomSearch } = useQuery({
    queryKey: ['bom-masters', attachStyleNo, attachArticle],
    enabled: false,
    queryFn: async () => {
      const res = await bomMasterApi.search({ styleNo: attachStyleNo, article: attachArticle });
      return res?.data as BomMasterListItem[];
    },
  });

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

  if (draft !== null) {
    const headerStyleNo = getHeaderValue(draft.headerRows, 'Style No');
    const headerArticle = getHeaderValue(draft.headerRows, 'Article');

    if (headerStyleNo && !manualStyleNo) setManualStyleNo(headerStyleNo);
    if (headerArticle && !manualArticle) setManualArticle(headerArticle);
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

  const { mutate: attachBom, isPending: isAttachPending } = useMutation({
    mutationFn: async (bomId: number) => {
      if (!draft) throw new Error('Draft not loaded');

      const nextDraft = {
        ...(draft as any),
        system: {
          ...((draft as any)?.system || {}),
          bomMasterId: bomId,
        },
      };

      await ocrDraftApi.update(id, {
        sourceFilename: data?.sourceFilename,
        soNumber: data?.soNumber,
        draft: nextDraft,
      });

      setDraft(nextDraft);
      setIsAttachOpen(false);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['ocr-drafts'] });
      qc.invalidateQueries({ queryKey: ['ocr-draft', id] });
    },
    onError: (err: any) => {
      const msg = err?.response?.data?.message || err?.message || 'Attach BoM failed';
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
          <div className='text-sm text-gray-500'>OCR Draft ID: {id}</div>
          <h1 className='text-2xl font-bold text-gray-900'>BoM Master from OCR</h1>
          <div className='text-sm text-gray-600'>Status: {status}</div>
          {currentBomMasterId !== undefined && currentBomMasterId !== null && String(currentBomMasterId).trim() !== '' && (
            <div className='text-sm text-gray-600'>Attached BoM Master ID: {String(currentBomMasterId)}</div>
          )}
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
          <Button
            className='bg-indigo-600 hover:bg-indigo-700'
            onClick={() => setIsAttachOpen((v) => !v)}
            disabled={isAttachPending}
          >
            Attach BoM Master
          </Button>
          <Button
            className='bg-indigo-600 hover:bg-indigo-700'
            onClick={() => saveAsBomMaster()}
            disabled={isSaveAsBomPending}
          >
            {isSaveAsBomPending ? 'Saving as BoM...' : 'Save as BoM Master'}
          </Button>
        </div>
      </div>

      <div className='bg-white rounded-lg border border-gray-200 p-4'>
        <div className='grid grid-cols-1 md:grid-cols-2 gap-3'>
          <div className='space-y-1'>
            <div className='text-xs font-medium text-gray-600'>Style No</div>
            <input
              className='w-full border border-gray-200 rounded px-2 py-1 text-sm'
              value={manualStyleNo}
              onChange={(e) => setManualStyleNo(e.target.value)}
              placeholder='Style No'
            />
          </div>
          <div className='space-y-1'>
            <div className='text-xs font-medium text-gray-600'>Article</div>
            <input
              className='w-full border border-gray-200 rounded px-2 py-1 text-sm'
              value={manualArticle}
              onChange={(e) => setManualArticle(e.target.value)}
              placeholder='Article'
            />
          </div>
        </div>
        <div className='mt-2 text-xs text-gray-500'>
          If header fields are missing/incorrect, fill these manually before clicking Save as BoM Master.
        </div>
      </div>

      {isAttachOpen && (
        <div className='bg-white rounded-lg border border-gray-200 p-4 space-y-3'>
          <div className='flex items-center justify-between'>
            <div className='font-semibold text-gray-900'>Select BoM Master</div>
            <Button variant='outline' onClick={() => setIsAttachOpen(false)}>
              Close
            </Button>
          </div>

          <div className='grid grid-cols-1 md:grid-cols-3 gap-2 items-end'>
            <div className='space-y-1'>
              <div className='text-xs font-medium text-gray-600'>Style No</div>
              <input
                className='w-full border border-gray-200 rounded px-2 py-1 text-sm'
                value={attachStyleNo}
                onChange={(e) => setAttachStyleNo(e.target.value)}
                placeholder='Style No'
              />
            </div>
            <div className='space-y-1'>
              <div className='text-xs font-medium text-gray-600'>Article</div>
              <input
                className='w-full border border-gray-200 rounded px-2 py-1 text-sm'
                value={attachArticle}
                onChange={(e) => setAttachArticle(e.target.value)}
                placeholder='Article'
              />
            </div>
            <Button onClick={() => refetchBomSearch()} disabled={isBomSearching}>
              {isBomSearching ? 'Searching...' : 'Search'}
            </Button>
          </div>

          <div className='overflow-x-auto'>
            <table className='min-w-full divide-y divide-gray-200'>
              <thead className='bg-gray-50'>
                <tr>
                  <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>ID</th>
                  <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Style No</th>
                  <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Article</th>
                  <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Revision</th>
                  <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Status</th>
                  <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Actions</th>
                </tr>
              </thead>
              <tbody className='bg-white divide-y divide-gray-100'>
                {(bomSearchData || []).length === 0 && (
                  <tr>
                    <td colSpan={6} className='px-3 py-4 text-sm text-gray-500'>
                      No results.
                    </td>
                  </tr>
                )}
                {(bomSearchData || []).map((b) => (
                  <tr key={b.id}>
                    <td className='px-3 py-2 text-sm text-gray-900'>{b.id}</td>
                    <td className='px-3 py-2 text-sm text-gray-700'>{b.styleNo}</td>
                    <td className='px-3 py-2 text-sm text-gray-700'>{b.article}</td>
                    <td className='px-3 py-2 text-sm text-gray-700'>{b.revision ?? '-'}</td>
                    <td className='px-3 py-2 text-sm text-gray-700'>{b.status ?? '-'}</td>
                    <td className='px-3 py-2 text-sm text-gray-700'>
                      <Button className='h-8 px-3 text-sm' onClick={() => attachBom(b.id)} disabled={isAttachPending}>
                        {isAttachPending ? 'Attaching...' : 'Attach'}
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

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
