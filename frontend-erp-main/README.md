# ERP 클라이언트 템플릿

React, TypeScript, Vite로 구축된 현대적인 ERP 시스템용 프론트엔드 템플릿입니다. 이 프로젝트는 **Feature-First(기능 중심)** 아키텍처를 채택하여 확장성과 유지보수성을 높였으며, 모듈화된 설계 패턴을 따르고 있습니다.

---

## 🚀 주요 특징

- **프레임워크**: React 18 + Vite (빠른 HMR 지원)
- **언어**: TypeScript
- **스타일링**: Tailwind CSS v4 + Vanilla CSS (글래스모피즘 디자인 시스템)
- **상태 관리**: React Context API (인증), TanStack Query (서버 상태)
- **라우팅**: React Router DOM v6
- **아이콘**: Lucide React
- **코드 품질**: ESLint + Prettier

---

## 🏗️ 아키텍처 및 프로젝트 구조

이 프로젝트는 **Feature-First (기능 중심)** 디렉토리 구조를 따릅니다. 파일의 종류(컴포넌트, 훅, 서비스 등)로 묶는 대신, **비즈니스 도메인(기능)** 별로 그룹화합니다. 이를 통해 관련된 코드를 한곳에서 관리할 수 있어 탐색과 확장이 용이합니다.

```
src/
├── api/                        # 공통 API 설정
│   └── client.ts               # Axios 인스턴스 및 인터셉터 설정
├── components/                 # 공통 UI 컴포넌트
│   ├── layout/                 # 레이아웃 컴포넌트 (Sidebar, MainLayout)
│   └── ui/                     # 재사용 가능한 UI 요소 (Button, Input)
├── features/                   # 기능 모듈 (도메인 로직)
│   ├── auth/                   # 인증 기능
│   │   ├── api.ts              # 인증 관련 API 호출
│   │   ├── AuthContext.tsx     # 인증 Provider 및 상태 관리
│   │   ├── LoginPage.tsx       # 페이지
│   │   └── SignupPage.tsx
│   ├── sales/                  # 수주 관리 기능
│   │   ├── api.ts
│   │   └── SalesOrderListPage.tsx
│   ├── inventory/              # 재고 관리 기능
│   │   ├── api.ts
│   │   └── StockListPage.tsx
│   ├── accounting/             # 회계 관리 기능
│       ├── api.ts
│       └── JournalEntryListPage.tsx
├── routes/                     # 라우팅 설정
│   └── index.tsx               # 라우트 정의 및 가드 설정
├── types/                      # 공통 글로벌 타입
│   └── index.ts                # API 응답 및 User 인터페이스
└── App.tsx                     # 애플리케이션 루트
```

---

## 🔄 핵심 개념 및 작동 흐름

### 1. 인증 (JWT)

이 애플리케이션은 **토큰 기반 인증(Token-based Authentication)** 시스템을 사용합니다.

- **로그인 흐름**:
  1.  사용자가 `LoginPage`에서 정보를 입력합니다.
  2.  `authApi.login`이 `POST /api/auth/login` 요청을 보냅니다.
  3.  성공 시, `accessToken`이 `localStorage`에 저장됩니다.
  4.  Axios 인터셉터(`src/api/client.ts`)가 이후의 모든 요청 헤더에 `Authorization: Bearer <token>`을 자동으로 추가합니다.
- **자동 로그아웃**:
  - API에서 `401 Unauthorized` 또는 `403 Forbidden` 응답이 오면(로그인/회원가입 요청 제외), 사용자는 자동으로 로그아웃되어 `/login` 페이지로 리다이렉트됩니다.

### 2. 데이터 페칭 (TanStack Query)

서버 상태 관리를 위해 **TanStack Query (React Query)** 를 사용합니다.

- **캐싱**: 데이터가 자동으로 캐싱되어 불필요한 API 호출을 줄입니다.
- **로딩/에러 상태**: 수동으로 상태를 관리할 필요 없이 선언적으로 처리됩니다.
- **사용 예시**:
  ```tsx
  // 기능 컴포넌트 내부
  const { data, isLoading, error } = useQuery({
    queryKey: ['sales', 'list'],
    queryFn: salesApi.getAll,
  });
  ```

### 3. API 통합 패턴

각 기능 모듈은 해당 기능에 특화된 API 메소드를 정의한 `api.ts` 파일을 가지고 있습니다.

- **표준 응답 래퍼**: 모든 API 응답은 `ApiResponse<T>` 타입으로 정의됩니다:
  ```ts
  interface ApiResponse<T> {
    success: boolean;
    data: T;
    error?: { code: string; message: string };
  }
  ```
- **캡슐화**: 컴포넌트는 `axios`를 직접 사용하지 않고, `salesApi.getAll()`과 같은 메서드를 통해 통신합니다.

---

## 🎨 UI/UX 디자인 시스템

시각적 언어는 **모던하고 깔끔하며 트렌디한** 느낌을 주도록 설계되었으며, **글래스모피즘(Glassmorphism)** 미학을 따릅니다.

- **색상 팔레트**:
  - **Primary**: Indigo (`indigo-600`) - 주요 액션 및 강조 상태에 사용.
  - **배경**: 은은한 그라데이션과 반투명한 흰색 패널 (`bg-white/70 backdrop-blur-2xl`).
- **인터랙션**:
  - **호버 효과**: 버튼이 살짝 떠오르며(`-translate-y-1`) 그림자가 생깁니다.
  - **애니메이션**: 부드러운 전환(`transition-all duration-300`)과 진입 애니메이션(`animate-fade-in`).
- **접근성**:
  - 모든 상호작용 가능한 요소에는 `cursor-pointer`가 적용됩니다.
  - `focus:ring-2`를 통해 키보드 내비게이션을 지원합니다.

---

## ⚙️ 환경 설정 및 도구 가이드

개발 생산성과 코드 품질을 유지하기 위해 다음 도구들이 설정되어 있습니다.

### 1. Prettier (코드 포맷팅)

코드를 일관된 스타일로 자동 정리해 주는 도구입니다. `.prettierrc` 파일에서 규칙을 관리합니다.

- **설정 파일**: `.prettierrc`
- **주요 규칙**:
  - `"printWidth": 120` (한 줄 최대 길이 120자)
  - `"singleQuote": true` (작은따옴표 사용)
  - `"tabWidth": 2` (들여쓰기 2칸)
  - `"semi": true` (문장 끝 세미콜론 사용)
- **VS Code 설정 권장**: 저장 시 자동 포맷팅 기능을 켜두시면 편리합니다. (`"editor.formatOnSave": true`)

### 2. ESLint (코드 린팅)

코드의 잠재적인 오류를 찾고 품질을 높이는 도구입니다. `eslint.config.js` 파일에서 관리됩니다.

- **설정 파일**: `eslint.config.js`
- **주요 기능**:
  - **Import 정렬 ('simple-import-sort')**: import 구문을 자동으로 정렬합니다.
  - **React Hooks 검사**: Hook의 규칙 위반을 감지합니다.
  - **TypeScript 검사**: 사용하지 않는 변수 등을 경고합니다. (`_`로 시작하는 변수는 무시)

### 3. Tailwind CSS (스타일링)

유틸리티 클래스 기반의 CSS 프레임워크입니다. `tailwind.config.cjs`에서 커스텀 설정을 관리합니다.

- **설정 파일**: `tailwind.config.cjs`
- **커스텀 테마**:
  - **Colors**: `primary` (Indigo 계열), `secondary` (Slate 계열) 등 프로젝트 전용 컬러가 정의되어 있습니다.
  - **Font**: `Inter` 폰트를 기본으로 사용합니다.
  - **Animation**: `fade-in`, `slide-up` 등의 커스텀 애니메이션이 포함되어 있습니다.
- **사용법**: class 속성에 유틸리티 클래스를 조합하여 사용합니다.
  ```tsx
  <div className='bg-primary text-white p-4 rounded-lg hover:bg-primary-hover'>버튼 예시</div>
  ```

---

## 💻 시작하기 (Getting Started)

### 사전 요구사항

- Node.js (v18 이상)
- npm

### 설치

1.  **저장소 클론**

    ```bash
    git clone <repository-url>
    cd erp-client-template
    ```

2.  **의존성 설치**

    ```bash
    npm install
    ```

3.  **환경 설정**  
    로컬 백엔드 API를 연결해야할 경우,  
    `./env_variables/` 디렉토리 하위에 `.env.development.local` 파일을 생성하고 아래 내용을 입력합니다.  
    (.env.development.local 파일은 깃 저장소에 공유하지 않도록 합니다.)  
    ```
    VITE_API_URL=http://localhost:8080
    ``` 

4.  **개발 서버 실행**
    ```bash
    npm run dev
    ```

---

## 🛠️ 확장 가이드 (Developer Guide)

### 새로운 기능(Feature) 추가하기

1.  **디렉토리 생성**: `src/features/<새로운-기능-이름>/` 폴더를 만듭니다.
2.  **API 정의**: 해당 폴더 내에 `api.ts`를 만들고 필요한 엔드포인트를 정의합니다.
3.  **페이지/컴포넌트 생성**: `src/components/ui/`의 공통 컴포넌트를 사용하여 UI를 구성합니다 (예: `NewFeaturePage.tsx`).
4.  **라우트 등록**: `src/routes/index.tsx`에 새로운 페이지 경로를 추가합니다.

### 새로운 UI 컴포넌트 추가하기

1.  `src/components/ui/` 폴더에 컴포넌트를 생성합니다.
2.  `Tailwind CSS`를 사용하여 스타일링합니다.
3.  유연성을 위해 `className` prop을 받아 내부 스타일과 병합(`cn()` 유틸리티 사용 권장)하도록 작성합니다.
4.  **Named Export**로 내보냅니다.

---

## 📝 컨벤션 참고 (Conventions)

| 분류         | 규칙                     | 예시                                   |
| :----------- | :----------------------- | :------------------------------------- |
| **컴포넌트** | PascalCase, Named Export | `export function UserProfile() {}`     |
| **함수**     | camelCase                | `const handleSubmit = () => {}`        |
| **타입**     | PascalCase, 명확한 이름  | `interface SalesOrderRequest {}`       |
| **Hook**     | camelCase, 'use' 접두사  | `useAuth()`, `useSalesList()`          |
| **API**      | 객체로 캡슐화            | `authApi.login()`, `salesApi.getAll()` |

---
