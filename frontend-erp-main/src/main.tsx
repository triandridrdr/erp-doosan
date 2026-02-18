/**
 * @file main.tsx
 * @description 애플리케이션의 진입점(Entry Point)입니다.
 * React DOM을 초기화하고 최상위 컴포넌트인 App을 렌더링합니다.
 */
import './index.css';

import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

import App from './App.tsx';

// root 요소를 찾아 React 애플리케이션을 마운트합니다.
// StrictMode는 개발 모드에서 잠재적인 문제를 감지하기 위해 추가적인 검사를 수행합니다.
createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
