# API 요청/응답 샘플

이 문서는 ERP API Template의 모든 API 엔드포인트에 대한 요청/응답 예시를 제공합니다.

## 목차

1. [인증 (Auth)](#인증-auth)
2. [재고 관리 (Inventory)](#재고-관리-inventory)
3. [수주 관리 (Sales)](#수주-관리-sales)
4. [회계 관리 (Accounting)](#회계-관리-accounting)

---

## 인증 (Auth)

> **참고:** 인증 API를 제외한 모든 API는 JWT 토큰 인증이 필요합니다.
> 로그인 후 발급받은 토큰을 `Authorization` 헤더에 포함하여 요청해야 합니다.
>
> ```
> Authorization: Bearer {accessToken}
> ```

### 1. 회원가입

**Endpoint:** `POST /api/auth/signup`

**Request:**

```json
{
  "userId": "testuser",
  "password": "password123",
  "name": "홍길동"
}
```

**Response (201 Created):**

```json
{
  "success": true,
  "data": 1
}
```

**Validation Rules:**

- `userId`: 필수, 4~20자, 영문/숫자만 허용
- `password`: 필수, 최소 6자 이상
- `name`: 필수, 최대 50자

**Error Response (409 Conflict - 아이디 중복):**

```json
{
  "success": false,
  "error": {
    "code": "ERR-1100",
    "message": "이미 사용 중인 아이디입니다",
    "timestamp": "2025-12-12T10:00:00"
  }
}
```

---

### 2. 로그인

**Endpoint:** `POST /api/auth/login`

**Request:**

```json
{
  "userId": "testuser",
  "password": "password123"
}
```

**Response (200 OK):**

```json
{
  "success": true,
  "data": {
    "accessToken": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0dXNlciIsInJvbGUiOiJVU0VSIiwiaWF0IjoxNzM0MjM0NTY3LCJleHAiOjE3MzQyMzgxNjd9.signature",
    "tokenType": "Bearer",
    "expiresIn": 3600000
  }
}
```

**응답 필드 설명:**

- `accessToken`: JWT 액세스 토큰
- `tokenType`: 토큰 타입 (항상 "Bearer")
- `expiresIn`: 토큰 만료 시간 (밀리초, 기본 1시간)

**Error Response (401 Unauthorized - 인증 실패):**

```json
{
  "success": false,
  "error": {
    "code": "ERR-1101",
    "message": "아이디 또는 비밀번호가 올바르지 않습니다",
    "timestamp": "2025-12-12T10:00:00"
  }
}
```

---

### 3. 인증된 API 호출 예시

로그인 후 발급받은 토큰을 사용하여 보호된 API를 호출합니다.

**Request:**

```http
GET /api/v1/sales/orders HTTP/1.1
Host: localhost:8080
Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0dXNlciIsInJvbGUiOiJVU0VSIiwiaWF0IjoxNzM0MjM0NTY3LCJleHAiOjE3MzQyMzgxNjd9.signature
Content-Type: application/json
```

**Error Response (401 Unauthorized - 토큰 없음/유효하지 않음):**

```json
{
  "success": false,
  "error": {
    "code": "ERR-1004",
    "message": "인증이 필요합니다",
    "timestamp": "2025-12-12T10:00:00"
  }
}
```

---

## 재고 관리 (Inventory)

### 1. 재고 생성

**Endpoint:** `POST /api/v1/inventory/stocks`

**Request:**

```json
{
  "itemCode": "ITEM001",
  "itemName": "노트북",
  "warehouseCode": "WH-001",
  "warehouseName": "서울 본사 창고",
  "quantity": 100,
  "unit": "EA",
  "unitPrice": 1500000
}
```

**Response (201 Created):**

```json
{
  "success": true,
  "data": {
    "id": 1,
    "itemCode": "ITEM001",
    "itemName": "노트북",
    "warehouseCode": "WH-001",
    "warehouseName": "서울 본사 창고",
    "quantity": 100.0,
    "availableQuantity": 100.0,
    "reservedQuantity": 0.0,
    "unit": "EA",
    "unitPrice": 1500000.0,
    "createdAt": "2025-12-12T10:30:15",
    "createdBy": "system"
  },
  "message": "재고가 생성되었습니다"
}
```

**Error Response (409 Conflict - 중복):**

```json
{
  "success": false,
  "error": {
    "code": "ERR-1006",
    "message": "이미 존재하는 재고입니다 (품목: ITEM001, 창고: WH-001)",
    "timestamp": "2025-12-12T10:30:15"
  }
}
```

---

### 2. 재고 단건 조회

**Endpoint:** `GET /api/v1/inventory/stocks/{id}`

**Request:**

```
GET /api/v1/inventory/stocks/1
```

**Response (200 OK):**

```json
{
  "success": true,
  "data": {
    "id": 1,
    "itemCode": "ITEM001",
    "itemName": "노트북",
    "warehouseCode": "WH-001",
    "warehouseName": "서울 본사 창고",
    "quantity": 100.0,
    "availableQuantity": 90.0,
    "reservedQuantity": 10.0,
    "unit": "EA",
    "unitPrice": 1500000.0,
    "createdAt": "2025-12-12T10:30:15",
    "createdBy": "system"
  }
}
```

---

### 3. 전체 재고 조회

**Endpoint:** `GET /api/v1/inventory/stocks`

**Request:**

```
GET /api/v1/inventory/stocks
```

**Response (200 OK):**

```json
{
  "success": true,
  "data": [
    {
      "id": 1,
      "itemCode": "ITEM001",
      "itemName": "노트북",
      "warehouseCode": "WH-001",
      "warehouseName": "서울 본사 창고",
      "quantity": 100.0,
      "availableQuantity": 90.0,
      "reservedQuantity": 10.0,
      "unit": "EA",
      "unitPrice": 1500000.0,
      "createdAt": "2025-12-12T10:30:15",
      "createdBy": "system"
    },
    {
      "id": 2,
      "itemCode": "ITEM002",
      "itemName": "마우스",
      "warehouseCode": "WH-001",
      "warehouseName": "서울 본사 창고",
      "quantity": 500.0,
      "availableQuantity": 500.0,
      "reservedQuantity": 0.0,
      "unit": "EA",
      "unitPrice": 30000.0,
      "createdAt": "2025-12-12T11:00:00",
      "createdBy": "system"
    }
  ]
}
```

---

### 4. 품목별 재고 조회

**Endpoint:** `GET /api/v1/inventory/stocks/item/{itemCode}`

**Request:**

```
GET /api/v1/inventory/stocks/item/ITEM001
```

**Response (200 OK):**

```json
{
  "success": true,
  "data": [
    {
      "id": 1,
      "itemCode": "ITEM001",
      "itemName": "노트북",
      "warehouseCode": "WH-001",
      "warehouseName": "서울 본사 창고",
      "quantity": 100.0,
      "availableQuantity": 90.0,
      "reservedQuantity": 10.0,
      "unit": "EA",
      "unitPrice": 1500000.0,
      "createdAt": "2025-12-12T10:30:15",
      "createdBy": "system"
    },
    {
      "id": 3,
      "itemCode": "ITEM001",
      "itemName": "노트북",
      "warehouseCode": "WH-002",
      "warehouseName": "부산 지사 창고",
      "quantity": 50.0,
      "availableQuantity": 50.0,
      "reservedQuantity": 0.0,
      "unit": "EA",
      "unitPrice": 1500000.0,
      "createdAt": "2025-12-12T12:00:00",
      "createdBy": "system"
    }
  ]
}
```

---

### 5. 창고별 재고 조회

**Endpoint:** `GET /api/v1/inventory/stocks/warehouse/{warehouseCode}`

**Request:**

```
GET /api/v1/inventory/stocks/warehouse/WH-001
```

**Response (200 OK):**

```json
{
  "success": true,
  "data": [
    {
      "id": 1,
      "itemCode": "ITEM001",
      "itemName": "노트북",
      "warehouseCode": "WH-001",
      "warehouseName": "서울 본사 창고",
      "quantity": 100.0,
      "availableQuantity": 90.0,
      "reservedQuantity": 10.0,
      "unit": "EA",
      "unitPrice": 1500000.0,
      "createdAt": "2025-12-12T10:30:15",
      "createdBy": "system"
    },
    {
      "id": 2,
      "itemCode": "ITEM002",
      "itemName": "마우스",
      "warehouseCode": "WH-001",
      "warehouseName": "서울 본사 창고",
      "quantity": 500.0,
      "availableQuantity": 500.0,
      "reservedQuantity": 0.0,
      "unit": "EA",
      "unitPrice": 30000.0,
      "createdAt": "2025-12-12T11:00:00",
      "createdBy": "system"
    }
  ]
}
```

---

## 수주 관리 (Sales)

### 1. 수주 생성

**Endpoint:** `POST /api/v1/sales/orders`

**Request:**

```json
{
  "orderDate": "2025-12-12",
  "customerCode": "CUST001",
  "customerName": "두산중공업",
  "deliveryAddress": "서울시 강남구 테헤란로 123",
  "remarks": "긴급 주문",
  "lines": [
    {
      "lineNumber": 1,
      "itemCode": "ITEM001",
      "itemName": "노트북",
      "quantity": 10,
      "unitPrice": 1500000,
      "remarks": "모델: ThinkPad X1"
    },
    {
      "lineNumber": 2,
      "itemCode": "ITEM002",
      "itemName": "마우스",
      "quantity": 20,
      "unitPrice": 30000,
      "remarks": ""
    }
  ]
}
```

**Response (201 Created):**

```json
{
  "success": true,
  "data": {
    "id": 1,
    "orderNumber": "SO-20251212-0001",
    "orderDate": "2025-12-12",
    "customerCode": "CUST001",
    "customerName": "두산중공업",
    "status": "PENDING",
    "totalAmount": 15600000.0,
    "deliveryAddress": "서울시 강남구 테헤란로 123",
    "remarks": "긴급 주문",
    "lines": [
      {
        "id": 1,
        "lineNumber": 1,
        "itemCode": "ITEM001",
        "itemName": "노트북",
        "quantity": 10.0,
        "unitPrice": 1500000.0,
        "lineAmount": 15000000.0,
        "remarks": "모델: ThinkPad X1"
      },
      {
        "id": 2,
        "lineNumber": 2,
        "itemCode": "ITEM002",
        "itemName": "마우스",
        "quantity": 20.0,
        "unitPrice": 30000.0,
        "lineAmount": 600000.0,
        "remarks": ""
      }
    ],
    "createdAt": "2025-12-12T10:45:30",
    "createdBy": "system"
  },
  "message": "수주가 생성되었습니다"
}
```

**참고:**

- 수주 생성 시 `SalesOrderCreatedEvent`가 발행됩니다
- Inventory 모듈의 `SalesOrderEventListener`가 이벤트를 수신하여 자동으로 재고 예약을 처리합니다

---

### 2. 수주 단건 조회

**Endpoint:** `GET /api/v1/sales/orders/{id}`

**Request:**

```
GET /api/v1/sales/orders/1
```

**Response (200 OK):**

```json
{
  "success": true,
  "data": {
    "id": 1,
    "orderNumber": "SO-20251212-0001",
    "orderDate": "2025-12-12",
    "customerCode": "CUST001",
    "customerName": "두산중공업",
    "status": "CONFIRMED",
    "totalAmount": 15600000.0,
    "deliveryAddress": "서울시 강남구 테헤란로 123",
    "remarks": "긴급 주문",
    "lines": [
      {
        "id": 1,
        "lineNumber": 1,
        "itemCode": "ITEM001",
        "itemName": "노트북",
        "quantity": 10.0,
        "unitPrice": 1500000.0,
        "lineAmount": 15000000.0,
        "remarks": "모델: ThinkPad X1"
      },
      {
        "id": 2,
        "lineNumber": 2,
        "itemCode": "ITEM002",
        "itemName": "마우스",
        "quantity": 20.0,
        "unitPrice": 30000.0,
        "lineAmount": 600000.0,
        "remarks": ""
      }
    ],
    "createdAt": "2025-12-12T10:45:30",
    "createdBy": "system"
  }
}
```

---

### 3. 수주 목록 조회 (페이징)

**Endpoint:** `GET /api/v1/sales/orders`

**Request:**

```
GET /api/v1/sales/orders?page=0&size=20
```

**Response (200 OK):**

```json
{
  "success": true,
  "data": {
    "content": [
      {
        "id": 1,
        "orderNumber": "SO-20251212-0001",
        "orderDate": "2025-12-12",
        "customerCode": "CUST001",
        "customerName": "두산중공업",
        "status": "CONFIRMED",
        "totalAmount": 15600000.00,
        "deliveryAddress": "서울시 강남구 테헤란로 123",
        "remarks": "긴급 주문",
        "lines": [...],
        "createdAt": "2025-12-12T10:45:30",
        "createdBy": "system"
      },
      {
        "id": 2,
        "orderNumber": "SO-20251212-0002",
        "orderDate": "2025-12-12",
        "customerCode": "CUST002",
        "customerName": "현대건설",
        "status": "PENDING",
        "totalAmount": 5000000.00,
        "deliveryAddress": "서울시 서초구",
        "remarks": "",
        "lines": [...],
        "createdAt": "2025-12-12T11:30:00",
        "createdBy": "system"
      }
    ],
    "page": 0,
    "size": 20,
    "totalElements": 2,
    "totalPages": 1,
    "first": true,
    "last": true
  }
}
```

---

### 4. 수주 수정

**Endpoint:** `PUT /api/v1/sales/orders/{id}`

**Request:**

```json
{
  "orderDate": "2025-12-12",
  "customerCode": "CUST001",
  "customerName": "두산중공업 (수정)",
  "deliveryAddress": "서울시 강남구 테헤란로 456",
  "remarks": "배송지 변경됨",
  "lines": [
    {
      "lineNumber": 1,
      "itemCode": "ITEM001",
      "itemName": "노트북",
      "quantity": 15,
      "unitPrice": 1500000,
      "remarks": "수량 증가"
    }
  ]
}
```

**Response (200 OK):**

```json
{
  "success": true,
  "data": {
    "id": 1,
    "orderNumber": "SO-20251212-0001",
    "orderDate": "2025-12-12",
    "customerCode": "CUST001",
    "customerName": "두산중공업 (수정)",
    "status": "PENDING",
    "totalAmount": 22500000.0,
    "deliveryAddress": "서울시 강남구 테헤란로 456",
    "remarks": "배송지 변경됨",
    "lines": [
      {
        "id": 3,
        "lineNumber": 1,
        "itemCode": "ITEM001",
        "itemName": "노트북",
        "quantity": 15.0,
        "unitPrice": 1500000.0,
        "lineAmount": 22500000.0,
        "remarks": "수량 증가"
      }
    ],
    "createdAt": "2025-12-12T10:45:30",
    "createdBy": "system"
  },
  "message": "수주가 수정되었습니다"
}
```

---

### 5. 수주 확정

**Endpoint:** `POST /api/v1/sales/orders/{id}/confirm`

**Request:**

```
POST /api/v1/sales/orders/1/confirm
```

**Response (200 OK):**

```json
{
  "success": true,
  "data": {
    "id": 1,
    "orderNumber": "SO-20251212-0001",
    "orderDate": "2025-12-12",
    "customerCode": "CUST001",
    "customerName": "두산중공업",
    "status": "CONFIRMED",
    "totalAmount": 15600000.00,
    "deliveryAddress": "서울시 강남구 테헤란로 123",
    "remarks": "긴급 주문",
    "lines": [...],
    "createdAt": "2025-12-12T10:45:30",
    "createdBy": "system"
  },
  "message": "수주가 확정되었습니다"
}
```

**Error Response (400 Bad Request - 이미 확정됨):**

```json
{
  "success": false,
  "error": {
    "code": "ERR-3002",
    "message": "이미 확정된 수주입니다",
    "timestamp": "2025-12-12T14:30:00"
  }
}
```

---

### 6. 수주 삭제 (Soft Delete)

**Endpoint:** `DELETE /api/v1/sales/orders/{id}`

**Request:**

```
DELETE /api/v1/sales/orders/1
```

**Response (200 OK):**

```json
{
  "success": true,
  "data": null,
  "message": "수주가 삭제되었습니다"
}
```

---

## 회계 관리 (Accounting)

### 1. 회계전표 생성

**Endpoint:** `POST /api/v1/accounting/journal-entries`

**Request:**

```json
{
  "entryDate": "2025-12-12",
  "description": "매출 인식",
  "lines": [
    {
      "lineNumber": 1,
      "accountCode": "1100",
      "accountName": "현금",
      "debit": 15600000,
      "credit": 0,
      "description": "제품 판매 대금 현금 수령"
    },
    {
      "lineNumber": 2,
      "accountCode": "4100",
      "accountName": "매출",
      "debit": 0,
      "credit": 15600000,
      "description": "제품 매출"
    }
  ]
}
```

**Response (201 Created):**

```json
{
  "success": true,
  "data": {
    "id": 1,
    "entryNumber": "JE-20251212-0001",
    "entryDate": "2025-12-12",
    "status": "DRAFT",
    "description": "매출 인식",
    "totalDebit": 15600000.0,
    "totalCredit": 15600000.0,
    "lines": [
      {
        "id": 1,
        "lineNumber": 1,
        "accountCode": "1100",
        "accountName": "현금",
        "debit": 15600000.0,
        "credit": 0.0,
        "description": "제품 판매 대금 현금 수령"
      },
      {
        "id": 2,
        "lineNumber": 2,
        "accountCode": "4100",
        "accountName": "매출",
        "debit": 0.0,
        "credit": 15600000.0,
        "description": "제품 매출"
      }
    ],
    "createdAt": "2025-12-12T15:00:00",
    "createdBy": "system"
  },
  "message": "회계전표가 생성되었습니다"
}
```

**Error Response (400 Bad Request - 차대 불일치):**

```json
{
  "success": false,
  "error": {
    "code": "ERR-2003",
    "message": "차변과 대변이 일치하지 않습니다",
    "timestamp": "2025-12-12T15:00:00"
  }
}
```

---

### 2. 회계전표 단건 조회

**Endpoint:** `GET /api/v1/accounting/journal-entries/{id}`

**Request:**

```
GET /api/v1/accounting/journal-entries/1
```

**Response (200 OK):**

```json
{
  "success": true,
  "data": {
    "id": 1,
    "entryNumber": "JE-20251212-0001",
    "entryDate": "2025-12-12",
    "status": "POSTED",
    "description": "매출 인식",
    "totalDebit": 15600000.0,
    "totalCredit": 15600000.0,
    "lines": [
      {
        "id": 1,
        "lineNumber": 1,
        "accountCode": "1100",
        "accountName": "현금",
        "debit": 15600000.0,
        "credit": 0.0,
        "description": "제품 판매 대금 현금 수령"
      },
      {
        "id": 2,
        "lineNumber": 2,
        "accountCode": "4100",
        "accountName": "매출",
        "debit": 0.0,
        "credit": 15600000.0,
        "description": "제품 매출"
      }
    ],
    "createdAt": "2025-12-12T15:00:00",
    "createdBy": "system"
  }
}
```

---

### 3. 회계전표 목록 조회 (페이징)

**Endpoint:** `GET /api/v1/accounting/journal-entries`

**Request:**

```
GET /api/v1/accounting/journal-entries?page=0&size=20
```

**Response (200 OK):**

```json
{
  "success": true,
  "data": {
    "content": [
      {
        "id": 1,
        "entryNumber": "JE-20251212-0001",
        "entryDate": "2025-12-12",
        "status": "POSTED",
        "description": "매출 인식",
        "totalDebit": 15600000.00,
        "totalCredit": 15600000.00,
        "lines": [...],
        "createdAt": "2025-12-12T15:00:00",
        "createdBy": "system"
      },
      {
        "id": 2,
        "entryNumber": "JE-20251212-0002",
        "entryDate": "2025-12-12",
        "status": "DRAFT",
        "description": "급여 지급",
        "totalDebit": 5000000.00,
        "totalCredit": 5000000.00,
        "lines": [...],
        "createdAt": "2025-12-12T16:00:00",
        "createdBy": "system"
      }
    ],
    "page": 0,
    "size": 20,
    "totalElements": 2,
    "totalPages": 1,
    "first": true,
    "last": true
  }
}
```

---

### 4. 회계전표 전기

**Endpoint:** `POST /api/v1/accounting/journal-entries/{id}/post`

**Request:**

```
POST /api/v1/accounting/journal-entries/1/post
```

**Response (200 OK):**

```json
{
  "success": true,
  "data": {
    "id": 1,
    "entryNumber": "JE-20251212-0001",
    "entryDate": "2025-12-12",
    "status": "POSTED",
    "description": "매출 인식",
    "totalDebit": 15600000.00,
    "totalCredit": 15600000.00,
    "lines": [...],
    "createdAt": "2025-12-12T15:00:00",
    "createdBy": "system"
  },
  "message": "회계전표가 전기되었습니다"
}
```

**Error Response (400 Bad Request - 이미 전기됨):**

```json
{
  "success": false,
  "error": {
    "code": "ERR-2002",
    "message": "이미 전기된 전표입니다",
    "timestamp": "2025-12-12T16:30:00"
  }
}
```

---

### 5. 회계전표 삭제 (Soft Delete)

**Endpoint:** `DELETE /api/v1/accounting/journal-entries/{id}`

**Request:**

```
DELETE /api/v1/accounting/journal-entries/1
```

**Response (200 OK):**

```json
{
  "success": true,
  "data": null,
  "message": "회계전표가 삭제되었습니다"
}
```

---

## 공통 에러 응답

### 1. 인증 필요 (401 Unauthorized)

```json
{
  "success": false,
  "error": {
    "code": "ERR-1004",
    "message": "인증이 필요합니다",
    "timestamp": "2025-12-12T10:00:00"
  }
}
```

### 2. 권한 없음 (403 Forbidden)

```json
{
  "success": false,
  "error": {
    "code": "ERR-1005",
    "message": "권한이 없습니다",
    "timestamp": "2025-12-12T10:00:00"
  }
}
```

### 3. 리소스를 찾을 수 없음 (404 Not Found)

```json
{
  "success": false,
  "error": {
    "code": "ERR-1002",
    "message": "요청한 리소스를 찾을 수 없습니다",
    "timestamp": "2025-12-12T10:00:00"
  }
}
```

### 4. 잘못된 입력 값 (400 Bad Request)

```json
{
  "success": false,
  "error": {
    "code": "ERR-1001",
    "message": "잘못된 입력 값입니다",
    "details": {
      "orderDate": "주문일자는 필수입니다",
      "customerCode": "고객코드는 필수입니다",
      "lines": "수주 라인은 최소 1개 이상이어야 합니다"
    },
    "timestamp": "2025-12-12T10:00:00"
  }
}
```

### 5. 재고 부족 (400 Bad Request)

```json
{
  "success": false,
  "error": {
    "code": "ERR-4003",
    "message": "재고가 부족합니다",
    "timestamp": "2025-12-12T10:00:00"
  }
}
```

### 6. 서버 내부 오류 (500 Internal Server Error)

```json
{
  "success": false,
  "error": {
    "code": "ERR-1000",
    "message": "서버 내부 오류가 발생했습니다",
    "timestamp": "2025-12-12T10:00:00"
  }
}
```

---

## 참고 사항

### 날짜 형식

- 모든 날짜는 ISO 8601 형식을 사용합니다
- 날짜: `YYYY-MM-DD` (예: `2025-12-12`)
- 일시: `YYYY-MM-DDTHH:mm:ss` (예: `2025-12-12T10:30:15`)

### 금액 형식

- 모든 금액은 `BigDecimal` 타입으로 소수점 2자리까지 표시됩니다
- 예: `1500000.00`

### 페이징

- `page`: 0부터 시작하는 페이지 번호 (기본값: 0)
- `size`: 페이지당 항목 수 (기본값: 20)

### 상태 코드

- `200 OK`: 요청 성공
- `201 Created`: 리소스 생성 성공
- `400 Bad Request`: 잘못된 요청
- `401 Unauthorized`: 인증 필요 또는 인증 실패
- `403 Forbidden`: 권한 없음
- `404 Not Found`: 리소스를 찾을 수 없음
- `409 Conflict`: 리소스 중복
- `500 Internal Server Error`: 서버 내부 오류

### Swagger UI

더 자세한 API 문서는 Swagger UI에서 확인할 수 있습니다:

```
http://localhost:8080/api/docs
```
