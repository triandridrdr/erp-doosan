# OCR Module Guide (Amazon Textract)

## 개요

Amazon Textract를 활용한 OCR(광학 문자 인식) 모듈입니다. 두 가지 API를 제공합니다:

| API | 용도 | Textract API |
|-----|------|--------------|
| `/api/v1/ocr/extract` | 단순 텍스트 추출 | DetectDocumentText |
| `/api/v1/ocr/analyze` | 테이블/폼 구조화 추출 | AnalyzeDocument |

---

## API 엔드포인트

### 1. 텍스트 추출 API (기본)

단순히 이미지에서 텍스트 라인을 추출합니다.

**엔드포인트**: `POST /api/v1/ocr/extract`

**요청**:
```http
POST /api/v1/ocr/extract
Content-Type: multipart/form-data
Authorization: Bearer {JWT_TOKEN}

file: (이미지 파일)
```

**응답**:
```json
{
  "success": true,
  "data": {
    "extractedText": "Purchase Order - Store\nH&M\nOrder No: 528003-1322",
    "blocks": [
      {
        "text": "Purchase Order - Store",
        "confidence": 99.5,
        "blockType": "LINE"
      }
    ],
    "averageConfidence": 98.7
  },
  "message": "텍스트 추출이 완료되었습니다"
}
```

---

### 2. 문서 분석 API (테이블/폼 추출)

테이블 데이터와 키-값 쌍(폼 필드)을 구조화된 형태로 추출합니다.

**엔드포인트**: `POST /api/v1/ocr/analyze`

**요청**:
```http
POST /api/v1/ocr/analyze
Content-Type: multipart/form-data
Authorization: Bearer {JWT_TOKEN}

file: (이미지 파일)
```

**응답 구조**:
```json
{
  "success": true,
  "data": {
    "extractedText": "전체 텍스트...",
    "lines": [...],
    "tables": [
      {
        "tableIndex": 0,
        "rowCount": 3,
        "columnCount": 4,
        "cells": [
          {
            "rowIndex": 1,
            "columnIndex": 1,
            "text": "Order No:",
            "confidence": 99.2,
            "isHeader": true
          }
        ],
        "rows": [
          ["Order No:", "Product No:", "PT Prod No:", "Date of Order:"],
          ["528003-1322", "1335456", "92938", "31 Oct, 2025"]
        ],
        "headerToFirstRowMap": {
          "Order No:": "528003-1322",
          "Product No:": "1335456",
          "PT Prod No:": "92938",
          "Date of Order:": "31 Oct, 2025"
        }
      }
    ],
    "keyValuePairs": [
      {
        "key": "Order No:",
        "value": "528003-1322",
        "keyConfidence": 99.1,
        "valueConfidence": 98.5
      },
      {
        "key": "Product No:",
        "value": "1335456",
        "keyConfidence": 98.9,
        "valueConfidence": 99.0
      }
    ],
    "formFields": {
      "Order No:": "528003-1322",
      "Product No:": "1335456",
      "PT Prod No:": "92938",
      "Date of Order:": "31 Oct, 2025",
      "Supplier Name:": "D & J TRADING CO.,LTD"
    },
    "averageConfidence": 98.5
  },
  "message": "문서 분석이 완료되었습니다"
}
```

---

## 응답 필드 상세 설명

### DocumentAnalysisResponse

| 필드 | 타입 | 설명 |
|------|------|------|
| `extractedText` | String | 전체 텍스트 (줄바꿈으로 연결) |
| `lines` | List<TextBlockDto> | 텍스트 라인 목록 |
| `tables` | List<TableDto> | 추출된 테이블 목록 |
| `keyValuePairs` | List<KeyValueDto> | 키-값 쌍 목록 (신뢰도 포함) |
| `formFields` | Map<String, String> | 키-값을 Map으로 제공 (편의용) |
| `averageConfidence` | Float | 평균 신뢰도 |

### TableDto

| 필드 | 타입 | 설명 |
|------|------|------|
| `tableIndex` | int | 테이블 인덱스 (0부터 시작) |
| `rowCount` | int | 행 개수 |
| `columnCount` | int | 열 개수 |
| `cells` | List<CellDto> | 셀 목록 (상세 정보) |
| `rows` | List<List<String>> | 2차원 배열 형태의 테이블 데이터 |
| `headerToFirstRowMap` | Map<String, String> | 헤더-첫번째 데이터 행 매핑 |

### KeyValueDto

| 필드 | 타입 | 설명 |
|------|------|------|
| `key` | String | 키 (레이블) |
| `value` | String | 값 |
| `keyConfidence` | Float | 키 인식 신뢰도 |
| `valueConfidence` | Float | 값 인식 신뢰도 |

---

## 사용 예시

### Purchase Order 이미지에서 데이터 추출

```java
// API 호출 후 응답에서 데이터 접근

// 방법 1: formFields Map 사용 (가장 간단)
String orderNo = response.getFormFields().get("Order No:");
String productNo = response.getFormFields().get("Product No:");
String supplierName = response.getFormFields().get("Supplier Name:");

// 방법 2: keyValuePairs 사용 (신뢰도 확인 필요시)
response.getKeyValuePairs().stream()
    .filter(kv -> kv.getKey().contains("Order No"))
    .findFirst()
    .ifPresent(kv -> {
        System.out.println("Order No: " + kv.getValue());
        System.out.println("신뢰도: " + kv.getValueConfidence());
    });

// 방법 3: 테이블 데이터 접근
TableDto table = response.getTables().get(0);
List<List<String>> rows = table.getRows();

// 헤더-값 매핑으로 접근
Map<String, String> headerMap = table.getHeaderToFirstRowMap();
String quantity = headerMap.get("Quantity");
```

---

## 아키텍처 구조

```
┌─────────────────────────────────────────────────────────────────┐
│                         OCR Module                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌────────────────┐                                             │
│  │ OcrController  │  ← HTTP 요청 수신 (multipart/form-data)     │
│  │                │                                             │
│  │ /extract       │  → 단순 텍스트 추출                         │
│  │ /analyze       │  → 테이블/폼 분석                           │
│  └───────┬────────┘                                             │
│          │                                                       │
│          ▼                                                       │
│  ┌────────────────┐                                             │
│  │  OcrService    │  ← 파일 검증, 응답 변환                      │
│  │                │                                             │
│  │ extractText()  │  → DetectDocumentText API                   │
│  │ analyzeDocument() → AnalyzeDocument API                      │
│  └───────┬────────┘                                             │
│          │                                                       │
│          ▼                                                       │
│  ┌────────────────────┐                                         │
│  │ AwsTextractClient  │  ← AWS Textract API 호출                │
│  │                    │                                         │
│  │ detectDocumentText() → 텍스트 감지                           │
│  │ analyzeDocument()    → 문서 분석 (TABLES, FORMS)             │
│  └───────┬────────────┘                                         │
│          │                                                       │
└──────────┼──────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      AWS Textract Service                        │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  DetectDocumentText API                                  │    │
│  │  - 이미지에서 텍스트 감지                                 │    │
│  │  - 블록 단위로 결과 반환 (PAGE, LINE, WORD)              │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  AnalyzeDocument API (TABLES, FORMS)                     │    │
│  │  - 테이블 구조 인식 (TABLE, CELL)                        │    │
│  │  - 폼 필드 인식 (KEY_VALUE_SET)                          │    │
│  │  - 행/열 인덱스, 헤더 정보 포함                          │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Block Type 설명

Amazon Textract는 문서를 계층적 블록 구조로 반환합니다:

| BlockType | 설명 | API |
|-----------|------|-----|
| `PAGE` | 페이지 전체 | 공통 |
| `LINE` | 한 줄 텍스트 | 공통 |
| `WORD` | 개별 단어 | 공통 |
| `TABLE` | 표 영역 | AnalyzeDocument |
| `CELL` | 표의 셀 | AnalyzeDocument |
| `KEY_VALUE_SET` | 키-값 쌍 | AnalyzeDocument |

---

## 플로우 다이어그램

### 텍스트 추출 플로우 (/extract)

```
┌─────────────┐
│  클라이언트   │
└──────┬──────┘
       │ 1. POST /api/v1/ocr/extract
       ▼
┌─────────────────┐
│  OcrController  │
└────────┬────────┘
         │ 2. extractText(file)
         ▼
┌─────────────────┐
│   OcrService    │───→ 파일 검증 (형식, 크기)
└────────┬────────┘
         │ 3. detectDocumentText(bytes)
         ▼
┌─────────────────────┐
│  AwsTextractClient  │
└────────┬────────────┘
         │ 4. AWS API 호출
         ▼
┌─────────────────────┐
│   AWS Textract      │───→ DetectDocumentText
└────────┬────────────┘
         │ 5. LINE 블록 추출
         ▼
┌─────────────────┐
│   OcrResponse   │───→ extractedText, blocks
└────────┬────────┘
         │
         ▼
┌─────────────┐
│  클라이언트   │
└─────────────┘
```

### 문서 분석 플로우 (/analyze)

```
┌─────────────┐
│  클라이언트   │
└──────┬──────┘
       │ 1. POST /api/v1/ocr/analyze
       ▼
┌─────────────────┐
│  OcrController  │
└────────┬────────┘
         │ 2. analyzeDocument(file)
         ▼
┌─────────────────┐
│   OcrService    │───→ 파일 검증
└────────┬────────┘
         │ 3. analyzeDocument(bytes, [TABLES, FORMS])
         ▼
┌─────────────────────┐
│  AwsTextractClient  │
└────────┬────────────┘
         │ 4. AWS API 호출
         ▼
┌─────────────────────┐
│   AWS Textract      │───→ AnalyzeDocument
└────────┬────────────┘     (FeatureTypes: TABLES, FORMS)
         │
         ├─→ 5a. TABLE/CELL 블록 처리 → TableDto
         │
         ├─→ 5b. KEY_VALUE_SET 블록 처리 → KeyValueDto
         │
         └─→ 5c. LINE 블록 처리 → TextBlockDto
         ▼
┌──────────────────────────┐
│ DocumentAnalysisResponse │
│  - tables                │
│  - keyValuePairs         │
│  - formFields            │
│  - lines                 │
└────────┬─────────────────┘
         │
         ▼
┌─────────────┐
│  클라이언트   │
└─────────────┘
```

---

## 지원 파일 형식

- `image/png`
- `image/jpeg`
- `image/jpg`
- `application/pdf`

**파일 크기 제한**: 10MB (application.yml에서 설정)

---

## 에러 처리

### 에러 코드

| 에러 코드 | HTTP 상태 | 설명 |
|-----------|-----------|------|
| `ERR-5001` | 500 | OCR 처리 중 오류 발생 |
| `ERR-5002` | 400 | 지원하지 않는 파일 형식 |
| `ERR-5003` | 400 | 파일이 비어있음 |

### 에러 응답 예시

```json
{
  "success": false,
  "data": null,
  "message": "지원하지 않는 파일 형식입니다. 지원하는 파일 형식: PNG, JPG, JPEG, PDF",
  "timestamp": "2025-01-15T10:30:00"
}
```

---

## 주요 파일 목록

| 파일 | 역할 |
|------|------|
| `config/AwsConfig.java` | AWS Textract 클라이언트 Bean 설정 |
| `ocr/client/AwsTextractClient.java` | AWS Textract API 호출 래퍼 |
| `ocr/service/OcrService.java` | OCR 비즈니스 로직 |
| `ocr/controller/OcrController.java` | OCR API 엔드포인트 |
| `ocr/dto/OcrResponse.java` | 텍스트 추출 응답 DTO |
| `ocr/dto/DocumentAnalysisResponse.java` | 문서 분석 응답 DTO |
| `ocr/dto/TableDto.java` | 테이블 DTO |
| `ocr/dto/CellDto.java` | 셀 DTO |
| `ocr/dto/KeyValueDto.java` | 키-값 쌍 DTO |
| `ocr/dto/TextBlockDto.java` | 텍스트 블록 DTO |

---

## 설정

### application.yml

```yaml
# AWS 설정
aws:
  credentials:
    access-key: ${AWS_ACCESS_KEY_ID:}
    secret-key: ${AWS_SECRET_ACCESS_KEY:}
  region: ap-northeast-2

# 파일 업로드 설정
spring:
  servlet:
    multipart:
      max-file-size: 10MB
      max-request-size: 10MB
```

### AWS 자격증명 설정 방법

**방법 1: 환경변수 사용 (권장)**
```bash
export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
```

**방법 2: application.yml에 직접 설정**
```yaml
aws:
  credentials:
    access-key: YOUR_ACCESS_KEY
    secret-key: YOUR_SECRET_KEY
```

**방법 3: AWS 기본 자격증명 체인**
- `~/.aws/credentials` 파일 사용
- IAM Role (EC2, ECS 등)

---

## Swagger UI 테스트

1. 애플리케이션 실행
2. `http://localhost:8080/api/docs` 접속
3. JWT 토큰 획득 (POST /api/auth/login)
4. Authorize 버튼 클릭 → 토큰 입력
5. OCR API 테스트:
   - `POST /api/v1/ocr/extract` - 단순 텍스트 추출
   - `POST /api/v1/ocr/analyze` - 테이블/폼 분석

---

## API 선택 가이드

| 상황 | 권장 API |
|------|----------|
| 단순 텍스트만 필요한 경우 | `/extract` |
| 구조화된 데이터 필요 (표, 폼) | `/analyze` |
| Order No, Product No 등 특정 필드 추출 | `/analyze` |
| 송장, 영수증, 주문서 처리 | `/analyze` |
| 비용 최적화가 중요한 경우 | `/extract` (더 저렴) |

> **참고**: AnalyzeDocument API는 DetectDocumentText보다 비용이 높습니다.
> 단순 텍스트 추출만 필요한 경우 `/extract` API 사용을 권장합니다.
