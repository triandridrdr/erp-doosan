/**
 * @file features/ocr/OcrPage.tsx
 * @description OCR 기능을 제공하는 페이지 컴포넌트입니다.
 * 이미지 파일을 업로드하여 단순 텍스트 추출 또는 상세 문서 분석(테이블, Key-Value)을 수행합니다.
 */
import { useMutation } from '@tanstack/react-query';
import { AlertCircle, FileText, Loader2, Table as TableIcon, Type, Upload } from 'lucide-react';
import { useState } from 'react';

import { Button } from '../../components/ui/Button';
import { ocrApi } from './api';
import type { DocumentAnalysisResponseData, TableDto } from './types';

// OCR 모드 정의: 단순 추출(extract) vs 문서 분석(analyze)
type OcrMode = 'extract' | 'analyze';

export function OcrPage() {
  const [mode, setMode] = useState<OcrMode>('extract'); // 현재 선택된 모드
  const [selectedFile, setSelectedFile] = useState<File | null>(null); // 업로드된 파일
  const [previewUrl, setPreviewUrl] = useState<string | null>(null); // 이미지 미리보기 URL

  // 텍스트 추출 Mutation
  const {
    mutate: extractText,
    isPending: isExtractPending,
    data: extractResult,
    error: extractError,
    reset: resetExtract,
  } = useMutation({
    mutationFn: ocrApi.extract,
  });

  // 문서 분석 Mutation
  const {
    mutate: analyzeDoc,
    isPending: isAnalyzePending,
    data: analyzeResult,
    error: analyzeError,
    reset: resetAnalyze,
  } = useMutation({
    mutationFn: ocrApi.analyze,
  });

  const isPending = isExtractPending || isAnalyzePending;

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

                  <div>
                    <h3 className='text-sm font-medium text-gray-700 mb-2'>Full text</h3>
                    <div className='bg-gray-50 p-4 rounded-lg text-sm text-gray-800 whitespace-pre-wrap border border-gray-100 max-h-96 overflow-y-auto font-mono'>
                      {extractResult.data.extractedText}
                    </div>
                  </div>

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
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
