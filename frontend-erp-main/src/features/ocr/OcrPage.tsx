/**
 * @file features/ocr/OcrPage.tsx
 * @description OCR 기능을 제공하는 페이지 컴포넌트입니다.
 * 이미지 파일을 업로드하여 단순 텍스트 추출 또는 상세 문서 분석(테이블, Key-Value)을 수행합니다.
 */
import { useMutation } from '@tanstack/react-query';
import { AlertCircle, FileText, Loader2, Table as TableIcon, Type, Upload } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';

import { Button } from '../../components/ui/Button';
import { ocrPythonApi } from './api';
import type {
  DocumentAnalysisResponse,
  DocumentAnalysisResponseData,
  OcrResponse,
  SalesOrderPayload,
  TableDto,
} from './types';

type ErpHeaderRow = { field: string; value: string; editable: boolean };
type ErpSizeRow = {
  id: string;
  color: string;
  xs: number;
  s: number;
  m: number;
  l: number;
  xl: number;
  total: number;
  editable: boolean;
};

type ErpBomRow = {
  id: string;
  component: string;
  category: string;
  composition: string;
  uom: string;
  consumptionPerUnit: string;
  wastePercent: string;
  editable: boolean;
};

type ErpSystemStatus = {
  status: 'DRAFT';
  soValidation: 'SUCCESS' | 'ERROR';
  bomStatus: 'INCOMPLETE';
  source: 'OCR JSON';
  warnings: string[];
};

type ErpDraft = {
  headerRows: ErpHeaderRow[];
  sizeRows: ErpSizeRow[];
  bomRows: ErpBomRow[];
  system: ErpSystemStatus;
  tracking: {
    salesOrderDraft: Record<string, unknown>;
    salesOrderLines: Array<Record<string, unknown>>;
    bomDraft: Array<Record<string, unknown>>;
  };
};

const asString = (v: unknown) => (v === null || v === undefined ? '' : String(v));

const newRowId = () => {
  try {
    const c = (globalThis as any)?.crypto;
    if (c && typeof c.randomUUID === 'function') return c.randomUUID();
  } catch {
    // ignore
  }
  return `${Date.now()}_${Math.random().toString(16).slice(2)}`;
};

const toIntLoose = (v: unknown) => {
  const s0 = asString(v).trim();
  if (!s0) return 0;
  const s = s0.replace(/,/g, '').replace(/\s+/g, '');
  const m = s.match(/-?\d+/);
  if (!m) return 0;
  const n = Number.parseInt(m[0], 10);
  return Number.isFinite(n) ? n : 0;
};

const pickAny = (obj: unknown, keys: string[]) => {
  if (!obj || typeof obj !== 'object') return undefined;
  const o = obj as Record<string, unknown>;
  for (const k of keys) {
    if (k in o) return o[k];
  }
  const lk = Object.keys(o);
  const wanted = new Set(keys.map((k) => k.toLowerCase()));
  for (const k of lk) {
    if (wanted.has(k.toLowerCase())) return o[k];
  }
  return undefined;
};

const parseToIsoDate = (raw: string) => {
  const s = (raw || '').trim();
  if (!s) return '';
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (iso) return s;
  const m = s.match(/^(\d{1,2})[\./-](\d{1,2})[\./-](\d{2,4})$/);
  if (!m) return s;
  const dd = Number.parseInt(m[1], 10);
  const mm = Number.parseInt(m[2], 10);
  let yy = Number.parseInt(m[3], 10);
  if (yy < 100) yy = 2000 + yy;
  if (!Number.isFinite(dd) || !Number.isFinite(mm) || !Number.isFinite(yy)) return s;
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${yy}-${pad(mm)}-${pad(dd)}`;
};

const inferBomCategoryAndUom = (component: string) => {
  const c = (component || '').toUpperCase();
  const isFabric =
    c.includes('FABRIC') ||
    c.includes('OUTER SHELL') ||
    c.includes('SHELL') ||
    c.includes('LINING') ||
    c.includes('MAIN') ||
    c.includes('SECONDARY');
  if (isFabric) return { category: 'FABRIC', uom: 'METER' };
  if (c.includes('TRIM') || c.includes('TRIMMING')) return { category: 'TRIMS', uom: 'PCS' };
  if (c.includes('EMBELLISH')) return { category: 'EMBELLISHMENT', uom: 'PCS' };
  return { category: 'TRIMS', uom: 'PCS' };
};

const parseCompositionPercentItems = (text: string) => {
  const out: string[] = [];
  const re = /(\d{1,3})\s*%\s*([A-Z][A-Z\s\-\/]*)/gi;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text || '')) !== null) {
    const pct = m[1];
    const mat = (m[2] || '').replace(/\s+/g, ' ').trim().toUpperCase();
    if (!pct || !mat) continue;
    out.push(`${pct}% ${mat}`);
  }
  if (out.length > 0) return out;

  const raw = (text || '').trim();
  if (!raw) return [];
  const parts = raw
    .split(',')
    .map((p) => p.trim())
    .filter(Boolean);
  return parts.length > 0 ? parts : [raw];
};

const buildErpDraft = (payload: SalesOrderPayload): ErpDraft => {
  const header = (payload?.header || {}) as Record<string, unknown>;
  const ordernr = asString(pickAny(header, ['ordernr', 'order_nr', 'order_no', 'order']));
  const dateRaw = asString(pickAny(header, ['date', 'orderdate', 'docdate']));
  const season = asString(pickAny(header, ['season']));
  const buyer = asString(pickAny(header, ['buyer']));
  const supplier = asString(pickAny(header, ['supplier']));
  const article = asString(pickAny(header, ['article', 'style', 'styleno']));
  const paymentterms = asString(pickAny(header, ['paymentterms', 'payment_terms', 'terms']));
  const marketoforigin = asString(pickAny(header, ['marketoforigin', 'market_of_origin', 'countryoforigin', 'origin']));
  const totalorderInt = toIntLoose(pickAny(header, ['totalorder', 'total']));
  const compositionsinformation = asString(
    pickAny(header, ['compositionsinformation', 'compositioninformation', 'composition'])
  );

  const headerRows: ErpHeaderRow[] = [
    { field: 'SO Number', value: ordernr, editable: true },
    { field: 'Date (ISO)', value: parseToIsoDate(dateRaw), editable: true },
    { field: 'Season', value: season, editable: true },
    { field: 'Buyer Code', value: buyer, editable: true },
    { field: 'Supplier', value: supplier, editable: true },
    { field: 'Article', value: article, editable: true },
    { field: 'Payment Terms', value: paymentterms, editable: true },
    { field: 'Country of Origin', value: marketoforigin, editable: true },
    { field: 'Total Qty', value: String(totalorderInt), editable: true },
  ];

  const grid = ((payload?.total_order as any)?.grid || []) as Array<Record<string, unknown>>;
  const sizeRows: ErpSizeRow[] = grid.map((r) => {
    const color = asString(pickAny(r, ['COLOUR', 'colour', 'Color', 'color']));
    const xs = toIntLoose(pickAny(r, ['XS', 'xs']));
    const s = toIntLoose(pickAny(r, ['S', 's']));
    const m = toIntLoose(pickAny(r, ['M', 'm']));
    const l = toIntLoose(pickAny(r, ['L', 'l']));
    const xl = toIntLoose(pickAny(r, ['XL', 'xl']));
    const total = xs + s + m + l + xl;
    return { id: newRowId(), color, xs, s, m, l, xl, total, editable: true };
  });

  const sumSizes = sizeRows.reduce((acc, r) => acc + (Number.isFinite(r.total) ? r.total : 0), 0);
  const warnings: string[] = [];
  if (totalorderInt > 0 && sumSizes !== totalorderInt) {
    const diff = sumSizes - totalorderInt;
    warnings.push(`VALIDATION WARNING: size breakdown total (${sumSizes}) != header total (${totalorderInt}). Diff=${diff}.`);
  }

  const components = ['MAIN FABRIC', 'SECONDARY FABRIC', 'EMBELLISHMENT', 'OUTER SHELL', 'TRIMMINGS'];
  const bomRowsRaw: ErpBomRow[] = [];

  const compText = compositionsinformation || '';
  const upper = compText.toUpperCase();
  const hits = components
    .map((k) => ({ k, i: upper.indexOf(k) }))
    .filter((x) => x.i >= 0)
    .sort((a, b) => a.i - b.i);

  if (hits.length > 0) {
    for (let idx = 0; idx < hits.length; idx++) {
      const start = hits[idx].i;
      const end = idx + 1 < hits.length ? hits[idx + 1].i : compText.length;
      const chunk = compText.slice(start, end).trim();
      const compKey = hits[idx].k;
      const items = parseCompositionPercentItems(chunk);
      const { category, uom } = inferBomCategoryAndUom(compKey);
      if (items.length > 0) {
        for (const it of items) {
          bomRowsRaw.push({
            id: newRowId(),
            component: compKey,
            category,
            composition: (it || '').replace(/,/g, '').trim(),
            uom,
            consumptionPerUnit: '',
            wastePercent: '',
            editable: true,
          });
        }
      } else {
        bomRowsRaw.push({
          id: newRowId(),
          component: compKey,
          category,
          composition: chunk.replace(/,/g, '').trim(),
          uom,
          consumptionPerUnit: '',
          wastePercent: '',
          editable: true,
        });
      }
    }
  } else if (compText.trim()) {
    const items = parseCompositionPercentItems(compText);
    const { category, uom } = inferBomCategoryAndUom('MAIN FABRIC');
    if (items.length > 0) {
      for (const it of items) {
        bomRowsRaw.push({
          id: newRowId(),
          component: 'MAIN FABRIC',
          category,
          composition: (it || '').replace(/,/g, '').trim(),
          uom,
          consumptionPerUnit: '',
          wastePercent: '',
          editable: true,
        });
      }
    } else {
      bomRowsRaw.push({
        id: newRowId(),
        component: 'MAIN FABRIC',
        category,
        composition: compText.replace(/,/g, '').trim(),
        uom,
        consumptionPerUnit: '',
        wastePercent: '',
        editable: true,
      });
    }
  }

  const system: ErpSystemStatus = {
    status: 'DRAFT',
    soValidation: warnings.length === 0 ? 'SUCCESS' : 'ERROR',
    bomStatus: 'INCOMPLETE',
    source: 'OCR JSON',
    warnings,
  };

  const salesOrderDraft = {
    status: 'DRAFT',
    so_number: ordernr,
    date_iso: parseToIsoDate(dateRaw),
    season,
    buyer_code: buyer,
    supplier,
    article,
    payment_terms: paymentterms,
    country_of_origin: marketoforigin,
    total_qty: totalorderInt,
    editable: true,
  };

  const salesOrderLines = sizeRows.map((r) => ({
    color: r.color,
    xs: r.xs,
    s: r.s,
    m: r.m,
    l: r.l,
    xl: r.xl,
    total: r.total,
    editable: true,
  }));

  const bomDraft = bomRowsRaw.map((r) => ({
    status: 'DRAFT',
    article,
    season,
    component: r.component,
    category: r.category,
    composition: r.composition,
    uom: r.uom,
    consumption_per_unit: null,
    waste_percent: null,
    editable: true,
  }));

  return {
    headerRows,
    sizeRows,
    bomRows: bomRowsRaw,
    system,
    tracking: { salesOrderDraft, salesOrderLines, bomDraft },
  };
};

// OCR 모드 정의: 단순 추출(extract) vs 문서 분석(analyze)
type OcrMode = 'extract' | 'analyze';

type OcrApiClient = {
  extract: (file: File) => Promise<OcrResponse>;
  analyze: (file: File) => Promise<DocumentAnalysisResponse>;
};

type OcrPageProps = {
  api?: OcrApiClient;
};

export function OcrPage({ api = ocrPythonApi }: OcrPageProps) {
  const [mode, setMode] = useState<OcrMode>('extract'); // 현재 선택된 모드
  const [selectedFile, setSelectedFile] = useState<File | null>(null); // 업로드된 파일
  const [previewUrl, setPreviewUrl] = useState<string | null>(null); // 이미지 미리보기 URL
  const [erpDraft, setErpDraft] = useState<ErpDraft | null>(null);

  // 텍스트 추출 Mutation
  const {
    mutate: extractText,
    isPending: isExtractPending,
    data: extractResult,
    error: extractError,
    reset: resetExtract,
  } = useMutation({
    mutationFn: api.extract,
  });

  // 문서 분석 Mutation
  const {
    mutate: analyzeDoc,
    isPending: isAnalyzePending,
    data: analyzeResult,
    error: analyzeError,
    reset: resetAnalyze,
  } = useMutation({
    mutationFn: api.analyze,
  });

  const isPending = isExtractPending || isAnalyzePending;

  const salesOrderPayload = useMemo(() => {
    const p = extractResult?.data?.salesOrderPayload;
    return p && typeof p === 'object' ? (p as SalesOrderPayload) : null;
  }, [extractResult?.data?.salesOrderPayload]);

  useEffect(() => {
    if (!salesOrderPayload) {
      setErpDraft(null);
      return;
    }
    try {
      setErpDraft(buildErpDraft(salesOrderPayload));
    } catch {
      setErpDraft(null);
    }
  }, [salesOrderPayload]);

  // 모드 변경 핸들러
  const handleModeChange = (newMode: OcrMode) => {
    setMode(newMode);
    // 모드 변경 시 이전 결과 초기화
    if (newMode === 'extract') resetAnalyze();
    else resetExtract();
  };

  // 파일 선택 핸들러
  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      const file = e.target.files[0];
      setSelectedFile(file);
      setPreviewUrl(URL.createObjectURL(file)); // 미리보기 URL 생성
      // 파일 변경 시 이전 결과 초기화
      resetExtract();
      resetAnalyze();
    }
  };

  // 처리 시작 핸들러
  const handleProcess = () => {
    if (selectedFile) {
      if (mode === 'extract') {
        extractText(selectedFile);
      } else {
        analyzeDoc(selectedFile);
      }
    }
  };

  // 문서 분석 결과 렌더링 함수
  const renderAnalysisResult = (data: DocumentAnalysisResponseData) => {
    // 첫 번째 줄을 문서 제목으로 추정하여 추출
    const documentTitle = data.extractedText
      .split('\n')
      .find((line) => line.trim().length > 0)
      ?.trim();

    return (
      <div className='space-y-8 animate-in fade-in slide-in-from-bottom-4 duration-500'>
        {/* 문서 제목 (추정) */}
        {documentTitle && (
          <div className='text-center pb-6 border-b border-gray-100'>
            <h2 className='text-2xl font-bold text-gray-800 break-words'>{documentTitle}</h2>
            <p className='text-sm text-gray-400 mt-2'>Document title (estimated)</p>
          </div>
        )}

        {/* 테이블 섹션 */}
        <div className='space-y-4'>
          <h3 className='font-bold text-lg text-gray-900 flex items-center'>
            <TableIcon className='w-5 h-5 mr-2' />
            Extracted tables ({data.tables.length})
          </h3>

          {data.tables.length > 0 ? (
            <div className='grid grid-cols-1 xl:grid-cols-2 gap-6'>
              {data.tables.map((table: TableDto, idx: number) => (
                <div key={idx} className='bg-white rounded-lg border border-gray-200 overflow-hidden shadow-sm'>
                  <div className='bg-gray-50 px-4 py-2 border-b border-gray-100 text-xs font-medium text-gray-500 uppercase tracking-wider'>
                    Table {idx + 1}
                  </div>
                  <div className='overflow-x-auto'>
                    <table className='min-w-full divide-y divide-gray-200'>
                      <tbody className='bg-white divide-y divide-gray-200'>
                        {table.rows.map((row, rIdx) => (
                          <tr key={rIdx} className={rIdx === 0 ? 'bg-gray-50/50' : ''}>
                            {row.map((cell, cIdx) => (
                              <td
                                key={cIdx}
                                className={`px-4 py-3 text-sm text-gray-700 whitespace-pre-wrap border-r border-gray-100 last:border-r-0 ${
                                  rIdx === 0 ? 'font-semibold text-gray-900' : ''
                                }`}
                              >
                                {cell}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className='bg-gray-50 rounded-lg p-8 text-center text-gray-500 border border-gray-200 border-dashed'>
              No tables detected.
            </div>
          )}
        </div>

        {/* 키-값 쌍 섹션 (Form Data) */}
        <div className='bg-white rounded-lg border border-gray-200 overflow-hidden'>
          <div className='bg-gray-50 px-4 py-3 border-b border-gray-200 flex justify-between items-center'>
            <h3 className='font-semibold text-gray-900'>Key-value details (Key-Value Pairs)</h3>
            <span className='text-xs text-gray-500'>Confidence scores shown</span>
          </div>
          <div className='max-h-96 overflow-y-auto p-4'>
            {data.keyValuePairs.length > 0 ? (
              <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4'>
                {data.keyValuePairs.map((kv, idx) => (
                  <div
                    key={idx}
                    className='p-3 rounded-lg border border-gray-100 hover:bg-gray-50 transition-colors flex justify-between items-start text-sm'
                  >
                    <div className='flex-1 pr-2'>
                      <span className='text-gray-500 text-xs block mb-1'>Key</span>
                      <span className='text-gray-700 font-medium break-words'>{kv.key}</span>
                    </div>
                    <div className='flex-1 text-right pl-2 border-l border-gray-100'>
                      <span className='text-gray-500 text-xs block mb-1'>Value</span>
                      <span className='text-gray-900 break-words'>{kv.value}</span>
                      <div className='mt-1 text-[10px] text-gray-400'>{Math.round(kv.valueConfidence)}%</div>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className='text-center text-gray-500 italic py-4'>No key-value pairs detected.</div>
            )}
          </div>
        </div>

        {/* 전체 텍스트 보기 토글 */}
        <div className='bg-gray-50 p-4 rounded-lg border border-gray-200'>
          <details className='group'>
            <summary className='flex justify-between items-center font-medium cursor-pointer list-none text-sm text-gray-700'>
              <span>View full text</span>
              <span className='transition group-open:rotate-180'>
                <svg
                  fill='none'
                  height='24'
                  shapeRendering='geometricPrecision'
                  stroke='currentColor'
                  strokeLinecap='round'
                  strokeLinejoin='round'
                  strokeWidth='1.5'
                  viewBox='0 0 24 24'
                  width='24'
                >
                  <path d='M6 9l6 6 6-6'></path>
                </svg>
              </span>
            </summary>
            <div className='text-neutral-600 mt-3 group-open:animate-fadeIn whitespace-pre-wrap text-xs font-mono p-2 bg-white rounded border border-gray-200'>
              {data.extractedText}
            </div>
          </details>
        </div>
      </div>
    );
  };

  return (
    <div className='space-y-8 max-w-screen-2xl mx-auto pb-20'>
      <div className='flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4'>
        <h1 className='text-2xl font-bold text-gray-900'>OCR Document Analysis</h1>

        {/* 모드 선택 탭 */}
        <div className='bg-gray-100 p-1 rounded-lg flex'>
          <button
            onClick={() => handleModeChange('extract')}
            className={`px-4 py-2 rounded-md text-sm font-medium transition-all ${
              mode === 'extract' ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-900'
            }`}
          >
            <span className='flex items-center'>
              <Type className='w-4 h-4 mr-2' />
              Simple Text
            </span>
          </button>
          <button
            onClick={() => handleModeChange('analyze')}
            className={`px-4 py-2 rounded-md text-sm font-medium transition-all ${
              mode === 'analyze' ? 'bg-white text-indigo-600 shadow-sm' : 'text-gray-500 hover:text-gray-900'
            }`}
          >
            <span className='flex items-center'>
              <TableIcon className='w-4 h-4 mr-2' />
              Table / Document Analysis
            </span>
          </button>
        </div>
      </div>

      <div className='flex flex-col gap-8'>
        {/* 상단: 파일 업로드 및 미리보기 섹션 */}
        <div className='bg-white p-6 rounded-lg shadow-sm border border-gray-200'>
          <h2 className='text-lg font-semibold text-gray-900 mb-4 flex items-center'>
            <Upload className='w-5 h-5 mr-2' />
            {mode === 'extract' ? 'Upload Image (Text Extraction)' : 'Upload Image (Document Analysis)'}
          </h2>

          <div className={`grid gap-6 ${selectedFile ? 'grid-cols-1 lg:grid-cols-2' : 'grid-cols-1'}`}>
            {/* 업로드 영역 */}
            <div className='flex flex-col'>
              <div className='flex-1 flex flex-col items-center justify-center border-2 border-dashed border-gray-300 rounded-lg p-10 hover:bg-gray-50 transition-colors relative bg-gray-50/50 min-h-96'>
                <input
                  type='file'
                  accept='image/*,application/pdf'
                  onChange={handleFileChange}
                  className='absolute inset-0 w-full h-full opacity-0 cursor-pointer'
                />
                {!selectedFile ? (
                  <div className='text-center text-gray-500'>
                    <FileText className='w-12 h-12 mx-auto mb-3 text-gray-400' />
                    <p className='text-sm font-medium'>Drag an image here or click to select a file</p>
                    <p className='text-xs mt-1 text-gray-400'>PNG, JPG, PDF (max 10MB)</p>
                  </div>
                ) : (
                  <div className='text-center'>
                    <p className='text-sm font-medium text-gray-900 mb-2'>Selected file</p>
                    <p className='text-xs text-gray-500 bg-white px-3 py-1 rounded border border-gray-200 inline-block'>
                      {selectedFile.name}
                    </p>
                    <p className='text-xs text-gray-400 mt-2'>Click to choose a different file</p>
                  </div>
                )}
              </div>
            </div>

            {/* 미리보기 영역 (파일 선택 시 표시) */}
            {selectedFile && previewUrl && (
              <div className='flex flex-col items-center justify-center bg-gray-900/5 rounded-lg border border-gray-200 p-4 min-h-96'>
                <img
                  src={previewUrl}
                  alt='Preview'
                  className='max-h-96 max-w-full object-contain rounded-md shadow-sm'
                />
              </div>
            )}
          </div>

          {/* 처리 버튼 */}
          <div className='mt-6 flex justify-end'>
            <Button
              onClick={handleProcess}
              disabled={!selectedFile || isPending}
              className={`w-full sm:w-auto h-12 px-8 text-base ${mode === 'analyze' ? 'bg-indigo-600 hover:bg-indigo-700' : ''}`}
            >
              {isPending ? (
                <>
                  <Loader2 className='w-5 h-5 mr-2 animate-spin' />
                  {mode === 'extract' ? 'Extracting text...' : 'Analyzing document...'}
                </>
              ) : (
                <>{mode === 'extract' ? 'Extract Text' : 'Analyze Tables and Data'}</>
              )}
            </Button>
          </div>
        </div>

        {/* 에러 메시지 표시 */}
        {(extractError || analyzeError) && (
          <div className='bg-red-50 border border-red-200 rounded-lg p-4 flex items-start animate-in fade-in slide-in-from-top-2'>
            <AlertCircle className='w-5 h-5 text-red-500 mr-2 flex-shrink-0 mt-0.5' />
            <div>
              <h3 className='text-sm font-medium text-red-800'>Request failed</h3>
              <p className='text-sm text-red-700 mt-1'>
                {(extractError as Error)?.message ||
                  (analyzeError as Error)?.message ||
                  'An unknown error occurred.'}
              </p>
            </div>
          </div>
        )}

        {/* 하단: 결과 표시 섹션 */}
        <div>
          {/* Analyze 모드 결과 */}
          {mode === 'analyze' && (
            <div className={`transition-all duration-500 ${analyzeResult ? 'opacity-100' : 'opacity-0'}`}>
              {analyzeResult?.data && (
                <div className='bg-white p-8 rounded-lg shadow-sm border border-gray-200'>
                  <div className='flex items-center justify-between mb-6'>
                    <h2 className='text-xl font-bold text-gray-900 flex items-center'>
                      <TableIcon className='w-6 h-6 mr-3 text-indigo-600' />
                      Analysis results
                    </h2>
                    <span className='text-sm font-medium text-indigo-600 bg-indigo-50 px-3 py-1 rounded-full border border-indigo-100'>
                      Average confidence: {analyzeResult.data.averageConfidence.toFixed(1)}%
                    </span>
                  </div>

                  {renderAnalysisResult(analyzeResult.data)}
                </div>
              )}
            </div>
          )}

          {/* Extract 모드 결과 */}
          {mode === 'extract' && (extractResult || isExtractPending) && (
            <div className='bg-white p-6 rounded-lg shadow-sm border border-gray-200 min-h-150'>
              <h2 className='text-lg font-semibold text-gray-900 mb-4 flex items-center'>
                <FileText className='w-5 h-5 mr-2' />
                Extraction result (simple text)
              </h2>

              {isPending && (
                <div className='flex flex-col items-center justify-center h-64 text-gray-500'>
                  <Loader2 className='w-8 h-8 animate-spin mb-4 text-indigo-500' />
                  <p>Analyzing text...</p>
                </div>
              )}

              {extractResult && extractResult.success && (
                <div className='space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500'>
                  <div className='bg-indigo-50 p-4 rounded-lg flex items-center justify-between'>
                    <span className='text-sm font-medium text-indigo-900'>Average confidence</span>
                    <span className='text-lg font-bold text-indigo-600'>
                      {extractResult.data.averageConfidence.toFixed(1)}%
                    </span>
                  </div>

                  {(() => {
                    const hasSalesOrderPayload = !!extractResult.data.salesOrderPayload;
                    return (
                      <>
                        {hasSalesOrderPayload && erpDraft && (
                          <div className='space-y-8'>
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
                                      <th className='px-4 py-2 text-left text-xs font-semibold text-gray-600'>Editable</th>
                                    </tr>
                                  </thead>
                                  <tbody className='bg-white divide-y divide-gray-100'>
                                    {erpDraft.headerRows.map((r) => (
                                      <tr key={r.field}>
                                        <td className='px-4 py-2 text-sm text-gray-700 whitespace-nowrap'>{r.field}</td>
                                        <td className='px-4 py-2 text-sm text-gray-900'>
                                          <input
                                            className='w-full border border-gray-200 rounded px-2 py-1 text-sm'
                                            value={r.value}
                                            onChange={(e) => {
                                              setErpDraft((cur) => {
                                                if (!cur) return cur;
                                                return {
                                                  ...cur,
                                                  headerRows: cur.headerRows.map((x) =>
                                                    x.field === r.field ? { ...x, value: e.target.value } : x
                                                  ),
                                                };
                                              });
                                            }}
                                          />
                                        </td>
                                        <td className='px-4 py-2 text-sm text-gray-700'>{r.editable ? 'TRUE' : 'FALSE'}</td>
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
                                      setErpDraft((cur) => {
                                        if (!cur) return cur;
                                        const next = [
                                          ...cur.sizeRows,
                                          { id: newRowId(), color: '', xs: 0, s: 0, m: 0, l: 0, xl: 0, total: 0, editable: true },
                                        ];
                                        const headerTotal = toIntLoose(
                                          cur.headerRows.find((x) => x.field === 'Total Qty')?.value
                                        );
                                        const sumSizes = next.reduce((acc, rr) => acc + rr.total, 0);
                                        const warnings: string[] = [];
                                        if (headerTotal > 0 && sumSizes !== headerTotal) {
                                          warnings.push(
                                            `VALIDATION WARNING: size breakdown total (${sumSizes}) != header total (${headerTotal}). Diff=${sumSizes - headerTotal}.`
                                          );
                                        }
                                        return {
                                          ...cur,
                                          sizeRows: next,
                                          system: {
                                            ...cur.system,
                                            soValidation: warnings.length === 0 ? 'SUCCESS' : 'ERROR',
                                            warnings,
                                          },
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
                                      <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Editable</th>
                                      <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Actions</th>
                                    </tr>
                                  </thead>
                                  <tbody className='bg-white divide-y divide-gray-100'>
                                    {erpDraft.sizeRows.map((r, idx) => (
                                      <tr key={r.id}>
                                        <td className='px-3 py-2 text-sm text-gray-700'>
                                          <input
                                            className='w-full border border-gray-200 rounded px-2 py-1 text-sm'
                                            value={r.color}
                                            onChange={(e) => {
                                              setErpDraft((cur) => {
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
                                                setErpDraft((cur) => {
                                                  if (!cur) return cur;
                                                  const next = [...cur.sizeRows];
                                                  const row = { ...next[idx], [k]: v } as ErpSizeRow;
                                                  row.total = row.xs + row.s + row.m + row.l + row.xl;
                                                  next[idx] = row;
                                                  const headerTotal = toIntLoose(
                                                    cur.headerRows.find((x) => x.field === 'Total Qty')?.value
                                                  );
                                                  const sumSizes = next.reduce((acc, rr) => acc + rr.total, 0);
                                                  const warnings: string[] = [];
                                                  if (headerTotal > 0 && sumSizes !== headerTotal) {
                                                    warnings.push(
                                                      `VALIDATION WARNING: size breakdown total (${sumSizes}) != header total (${headerTotal}). Diff=${sumSizes - headerTotal}.`
                                                    );
                                                  }
                                                  return {
                                                    ...cur,
                                                    sizeRows: next,
                                                    system: {
                                                      ...cur.system,
                                                      soValidation: warnings.length === 0 ? 'SUCCESS' : 'ERROR',
                                                      warnings,
                                                    },
                                                  };
                                                });
                                              }}
                                            />
                                          </td>
                                        ))}
                                        <td className='px-3 py-2 text-sm text-gray-900 font-semibold'>{r.total}</td>
                                        <td className='px-3 py-2 text-sm text-gray-700'>{r.editable ? 'TRUE' : 'FALSE'}</td>
                                        <td className='px-3 py-2 text-sm text-gray-700'>
                                          <Button
                                            className='h-8 px-3 text-sm bg-red-600 hover:bg-red-700'
                                            onClick={() => {
                                              setErpDraft((cur) => {
                                                if (!cur) return cur;
                                                const next = cur.sizeRows.filter((x) => x.id !== r.id);
                                                const headerTotal = toIntLoose(
                                                  cur.headerRows.find((x) => x.field === 'Total Qty')?.value
                                                );
                                                const sumSizes = next.reduce((acc, rr) => acc + rr.total, 0);
                                                const warnings: string[] = [];
                                                if (headerTotal > 0 && sumSizes !== headerTotal) {
                                                  warnings.push(
                                                    `VALIDATION WARNING: size breakdown total (${sumSizes}) != header total (${headerTotal}). Diff=${sumSizes - headerTotal}.`
                                                  );
                                                }
                                                return {
                                                  ...cur,
                                                  sizeRows: next,
                                                  system: {
                                                    ...cur.system,
                                                    soValidation: warnings.length === 0 ? 'SUCCESS' : 'ERROR',
                                                    warnings,
                                                  },
                                                };
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
                                      setErpDraft((cur) => {
                                        if (!cur) return cur;
                                        const next: ErpBomRow[] = [
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
                                        ];
                                        return { ...cur, bomRows: next };
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
                                      <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Editable</th>
                                      <th className='px-3 py-2 text-left text-xs font-semibold text-gray-600'>Actions</th>
                                    </tr>
                                  </thead>
                                  <tbody className='bg-white divide-y divide-gray-100'>
                                    {erpDraft.bomRows.length === 0 && (
                                      <tr>
                                        <td className='px-3 py-3 text-sm text-gray-500 italic' colSpan={8}>
                                          No compositionsinformation detected.
                                        </td>
                                      </tr>
                                    )}
                                    {erpDraft.bomRows.map((r, idx) => (
                                      <tr key={r.id}>
                                        <td className='px-3 py-2 text-sm text-gray-700'>
                                          <input
                                            className='w-56 border border-gray-200 rounded px-2 py-1 text-sm'
                                            value={r.component}
                                            onChange={(e) => {
                                              setErpDraft((cur) => {
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
                                              setErpDraft((cur) => {
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
                                              setErpDraft((cur) => {
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
                                              setErpDraft((cur) => {
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
                                              setErpDraft((cur) => {
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
                                              setErpDraft((cur) => {
                                                if (!cur) return cur;
                                                const next = [...cur.bomRows];
                                                next[idx] = { ...next[idx], wastePercent: e.target.value };
                                                return { ...cur, bomRows: next };
                                              });
                                            }}
                                          />
                                        </td>
                                        <td className='px-3 py-2 text-sm text-gray-700'>{r.editable ? 'TRUE' : 'FALSE'}</td>
                                        <td className='px-3 py-2 text-sm text-gray-700'>
                                          <Button
                                            className='h-8 px-3 text-sm bg-red-600 hover:bg-red-700'
                                            onClick={() => {
                                              setErpDraft((cur) => {
                                                if (!cur) return cur;
                                                const next = cur.bomRows.filter((x) => x.id !== r.id);
                                                return { ...cur, bomRows: next };
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
                                <h3 className='font-semibold text-gray-900'>SECTION 4 – SYSTEM STATUS</h3>
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
                                    <tr>
                                      <td className='px-4 py-2 text-sm text-gray-700'>Status</td>
                                      <td className='px-4 py-2 text-sm text-gray-900'>{erpDraft.system.status}</td>
                                    </tr>
                                    <tr>
                                      <td className='px-4 py-2 text-sm text-gray-700'>SO Validation</td>
                                      <td className='px-4 py-2 text-sm text-gray-900'>{erpDraft.system.soValidation}</td>
                                    </tr>
                                    <tr>
                                      <td className='px-4 py-2 text-sm text-gray-700'>BoM Status</td>
                                      <td className='px-4 py-2 text-sm text-gray-900'>{erpDraft.system.bomStatus}</td>
                                    </tr>
                                    <tr>
                                      <td className='px-4 py-2 text-sm text-gray-700'>Source</td>
                                      <td className='px-4 py-2 text-sm text-gray-900'>{erpDraft.system.source}</td>
                                    </tr>
                                  </tbody>
                                </table>
                              </div>
                            </div>

                            {erpDraft.system.warnings.length > 0 && (
                              <div className='bg-amber-50 border border-amber-200 rounded-lg p-4'>
                                <div className='font-semibold text-amber-900 mb-2'>=== VALIDATION WARNING ===</div>
                                <div className='space-y-1'>
                                  {erpDraft.system.warnings.map((w, i) => (
                                    <div key={i} className='text-sm text-amber-900'>
                                      {w}
                                    </div>
                                  ))}
                                </div>
                              </div>
                            )}

                            <div className='bg-white rounded-lg border border-gray-200 overflow-hidden'>
                              <div className='bg-gray-50 px-4 py-3 border-b border-gray-200'>
                                <h3 className='font-semibold text-gray-900'>Output JSON (Tracking)</h3>
                              </div>
                              <div className='p-4 overflow-x-auto'>
                                <table className='min-w-full divide-y divide-gray-200'>
                                  <thead className='bg-gray-50'>
                                    <tr>
                                      <th className='px-4 py-2 text-left text-xs font-semibold text-gray-600'>Object</th>
                                      <th className='px-4 py-2 text-left text-xs font-semibold text-gray-600'>JSON</th>
                                    </tr>
                                  </thead>
                                  <tbody className='bg-white divide-y divide-gray-100'>
                                    {([
                                      ['Sales Order Draft', erpDraft.tracking.salesOrderDraft],
                                      ['Sales Order Lines', erpDraft.tracking.salesOrderLines],
                                      ['BoM Draft', erpDraft.tracking.bomDraft],
                                    ] as const).map(([name, obj]) => (
                                      <tr key={name}>
                                        <td className='px-4 py-2 text-sm text-gray-700 whitespace-nowrap'>{name}</td>
                                        <td className='px-4 py-2 text-sm text-gray-900'>
                                          <textarea
                                            className='w-full border border-gray-200 rounded px-2 py-1 text-xs font-mono'
                                            rows={6}
                                            value={JSON.stringify(obj, null, 2)}
                                            readOnly
                                          />
                                        </td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            </div>
                          </div>
                        )}

                        {/* Only show verbose OCR outputs when salesOrderPayload is NOT present */}
                        {!hasSalesOrderPayload && (
                          <>
                            <div>
                              <h3 className='text-sm font-medium text-gray-700 mb-2'>Full text</h3>
                              <div className='bg-gray-50 p-4 rounded-lg text-sm text-gray-800 whitespace-pre-wrap border border-gray-100 max-h-96 overflow-y-auto font-mono'>
                                {extractResult.data.extractedText}
                              </div>
                            </div>

                            {extractResult.data.tables && (
                              <div className='space-y-6'>
                                {extractResult.data.tables && extractResult.data.tables.length > 0 && (
                                  <div className='space-y-4'>
                                    <h3 className='font-bold text-lg text-gray-900 flex items-center'>
                                      <TableIcon className='w-5 h-5 mr-2' />
                                      AI Tables ({extractResult.data.tables.length})
                                    </h3>
                                    <div className='grid grid-cols-1 gap-6'>
                                      {extractResult.data.tables.map((table: TableDto, idx: number) => (
                                        <div
                                          key={idx}
                                          className='bg-white rounded-lg border border-gray-200 overflow-hidden shadow-sm'
                                        >
                                          <div className='bg-gray-50 px-4 py-2 border-b border-gray-100 text-xs font-medium text-gray-500 uppercase tracking-wider'>
                                            Table {idx + 1}
                                          </div>
                                          <div className='overflow-x-auto max-h-[70vh]'>
                                            <table className='min-w-max w-full divide-y divide-gray-200'>
                                              <tbody className='bg-white divide-y divide-gray-200'>
                                                {table.rows.map((row, rIdx) => (
                                                  <tr key={rIdx} className={rIdx === 0 ? 'bg-gray-50/50' : ''}>
                                                    {row.map((cell, cIdx) => (
                                                      <td
                                                        key={cIdx}
                                                        className={`px-4 py-3 text-sm text-gray-700 whitespace-nowrap border-r border-gray-100 last:border-r-0 ${
                                                          rIdx === 0 ? 'font-semibold text-gray-900' : ''
                                                        }`}
                                                      >
                                                        {cell}
                                                      </td>
                                                    ))}
                                                  </tr>
                                                ))}
                                              </tbody>
                                            </table>
                                          </div>
                                        </div>
                                      ))}
                                    </div>
                                  </div>
                                )}
                              </div>
                            )}

                            {/* 블록 상세 보기 */}
                            <details className='group'>
                              <summary className='text-sm font-medium text-gray-700 cursor-pointer mb-2 list-none flex items-center'>
                                <span>Detected blocks ({extractResult.data.blocks.length}) - view details</span>
                                <span className='ml-2 transition group-open:rotate-180 text-gray-400'>▼</span>
                              </summary>

                              <div className='border border-gray-200 rounded-lg overflow-hidden mt-2'>
                                <div className='max-h-60 overflow-y-auto divide-y divide-gray-100'>
                                  {extractResult.data.blocks.map((block, index) => (
                                    <div
                                      key={index}
                                      className='p-3 hover:bg-gray-50 transition-colors flex justify-between items-start'
                                    >
                                      <p className='text-sm text-gray-900 flex-1 mr-4'>{block.text}</p>
                                      <div className='flex flex-col items-end'>
                                        <span className='text-xs bg-gray-100 px-2 py-0.5 rounded text-gray-600 font-medium'>
                                          {block.blockType}
                                        </span>
                                        <span
                                          className={`text-xs mt-1 ${
                                            block.confidence > 90
                                              ? 'text-green-600'
                                              : block.confidence > 70
                                                ? 'text-yellow-600'
                                                : 'text-red-600'
                                          }`}
                                        >
                                          {block.confidence.toFixed(1)}%
                                        </span>
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            </details>
                          </>
                        )}
                      </>
                    );
                  })()}
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
