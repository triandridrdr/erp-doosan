import { ocrPythonApi } from './api';
import { OcrPage } from './OcrPage';

export function OcrPythonPage() {
  return <OcrPage api={ocrPythonApi} />;
}
