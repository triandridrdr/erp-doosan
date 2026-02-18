# ERP API Template 프로젝트 구조 및 작동 플로우

## 🎯 프로젝트 개요

### 목적

Modular Monolith 아키텍처 기반의 ERP 시스템 API 템플릿

### 핵심 특징

- **Modular Monolith**: 단일 애플리케이션 내에서 모듈별로 분리된 구조
- **도메인 주도 설계(DDD)**: 각 비즈니스 도메인별 독립적인 모듈
- **이벤트 기반 통신**: 모듈 간 느슨한 결합을 위한 도메인 이벤트 활용
- **PostgreSQL**: JPA/Hibernate를 통한 데이터 영속성

---

## 🏗️ 아키텍처 패턴

### Modular Monolith Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    ERP Application                      │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐            │
│  │  Sales   │  │Inventory │  │Accounting│            │
│  │  Module  │  │  Module  │  │  Module  │            │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘            │
│       │             │              │                   │
│       └─────────────┼──────────────┘                   │
│                     │                                   │
│              ┌──────▼──────┐                           │
│              │   Common    │                           │
│              │   Module    │                           │
│              └─────────────┘                           │
│                                                         │
├─────────────────────────────────────────────────────────┤
│              Infrastructure Layer                       │
│  (Database, Event Bus, Security, Config)               │
└─────────────────────────────────────────────────────────┘
```

### 계층형 아키텍처 (Layered Architecture)

각 모듈은 다음과 같은 계층 구조를 따릅니다:

```
Controller (Presentation Layer)
    ↓
Service (Business Logic Layer)
    ↓
Repository (Data Access Layer)
    ↓
Entity (Domain Layer)
```

---

## 📁 디렉토리 구조

```
src/main/java/com/doosan/erp/
│
├── common/                                 # 공통 모듈
│   ├── constant/
│   │   └── ErrorCode.java                 # 에러 코드 열거형
│   ├── dto/
│   │   ├── ApiResponse.java              # 표준 API 응답 형식
│   │   ├── ErrorResponse.java            # 에러 응답 형식
│   │   └── PageResponse.java             # 페이징 응답 형식
│   ├── entity/
│   │   └── BaseEntity.java               # 공통 엔티티 (ID, 생성일시 등)
│   ├── event/
│   │   └── DomainEvent.java              # 도메인 이벤트 마커 인터페이스
│   └── exception/
│       ├── BusinessException.java        # 비즈니스 예외
│       ├── ResourceNotFoundException.java
│       └── GlobalExceptionHandler.java   # 전역 예외 핸들러
│
├── auth/                                   # 인증 모듈
│   ├── controller/
│   │   └── AuthController.java           # 인증 API 컨트롤러
│   ├── service/
│   │   └── AuthService.java              # 인증 서비스
│   ├── repository/
│   │   └── UserRepository.java           # 사용자 Repository
│   ├── entity/
│   │   └── User.java                     # 사용자 엔티티
│   └── dto/
│       ├── LoginRequest.java             # 로그인 요청 DTO
│       ├── LoginResponse.java            # 로그인 응답 DTO (JWT 토큰)
│       └── SignupRequest.java            # 회원가입 요청 DTO
│
├── security/                               # 보안 관련 클래스
│   ├── JwtAuthenticationFilter.java       # JWT 인증 필터
│   └── JwtTokenProvider.java              # JWT 토큰 생성/검증
│
├── sales/                                 # 수주 관리 모듈
│   ├── controller/
│   │   └── SalesOrderController.java
│   ├── service/
│   │   └── SalesOrderService.java
│   ├── repository/
│   │   └── SalesOrderRepository.java
│   ├── entity/
│   │   ├── SalesOrder.java
│   │   └── SalesOrderLine.java
│   ├── dto/
│   │   ├── SalesOrderRequest.java
│   │   └── SalesOrderResponse.java
│   └── event/
│       └── SalesOrderCreatedEvent.java   # 수주 생성 이벤트
│
├── inventory/                             # 재고 관리 모듈
│   ├── controller/
│   │   └── StockController.java
│   ├── service/
│   │   └── StockService.java
│   ├── repository/
│   │   └── StockRepository.java
│   ├── entity/
│   │   └── Stock.java
│   ├── dto/
│   │   ├── StockRequest.java             # 재고 생성 요청 DTO
│   │   └── StockResponse.java
│   └── listener/
│       └── SalesOrderEventListener.java  # 수주 이벤트 리스너 (재고 예약)
│
├── accounting/                            # 회계 관리 모듈
│   ├── controller/
│   │   └── JournalEntryController.java
│   ├── service/
│   │   └── JournalEntryService.java
│   ├── repository/
│   │   └── JournalEntryRepository.java
│   ├── entity/
│   │   ├── JournalEntry.java
│   │   └── JournalEntryLine.java
│   ├── dto/
│   │   ├── JournalEntryRequest.java
│   │   └── JournalEntryResponse.java
│   └── event/
│       └── JournalEntryPostedEvent.java  # 전표 전기 이벤트
│
├── ocr/                                   # OCR 모듈 (Amazon Textract)
│   ├── client/
│   │   └── AwsTextractClient.java        # AWS Textract API 클라이언트
│   ├── controller/
│   │   └── OcrController.java            # OCR API 컨트롤러
│   ├── service/
│   │   └── OcrService.java               # OCR 비즈니스 로직
│   └── dto/
│       ├── OcrResponse.java              # OCR 응답 DTO
│       └── TextBlockDto.java             # 텍스트 블록 DTO
│
├── config/                                # 설정 클래스
│   ├── AsyncConfig.java                  # 비동기 처리 설정
│   ├── AwsConfig.java                    # AWS 설정 (Textract 클라이언트)
│   ├── JpaAuditingConfig.java            # JPA Auditing 설정
│   ├── SecurityConfig.java               # Spring Security 설정 (JWT 필터 적용)
│   ├── SwaggerConfig.java                # API 문서 설정
│   └── WebConfig.java                    # Web 설정 (CORS 등)
│
└── ErpApiTemplateApplication.java         # Spring Boot 메인 클래스

src/main/resources/
└── application.yml                        # 애플리케이션 설정

src/test/java/com/doosan/erp/
├── accounting/
│   └── controller/
│       └── JournalEntryControllerTest.java
├── inventory/
│   └── controller/
│       └── StockControllerTest.java
└── sales/
    └── controller/
        └── SalesOrderControllerTest.java
```

---

## 🔍 레이어별 상세 설명

### 1. Controller Layer (Presentation)

**역할**: HTTP 요청/응답 처리, 입력 검증, DTO 변환

**예시**: `SalesOrderController.java`

```java
@RestController
@RequestMapping("/api/v1/sales/orders")
public class SalesOrderController {

    @PostMapping
    public ResponseEntity<ApiResponse<SalesOrderResponse>> createSalesOrder(
            @Valid @RequestBody SalesOrderRequest request) {
        // 1. Request DTO 수신
        // 2. Service 호출
        // 3. Response DTO 반환
        SalesOrderResponse response = salesOrderService.createSalesOrder(request);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(ApiResponse.success(response));
    }
}
```

**특징**:

- `@RestController`: RESTful API 엔드포인트
- `@Valid`: Bean Validation을 통한 입력 검증
- `ApiResponse<T>`: 표준화된 응답 형식

---

### 2. Service Layer (Business Logic)

**역할**: 비즈니스 로직 수행, 트랜잭션 관리, 이벤트 발행

**예시**: `SalesOrderService.java`

```java
@Service
@Transactional(readOnly = true)
public class SalesOrderService {

    @Transactional
    public SalesOrderResponse createSalesOrder(SalesOrderRequest request) {
        // 1. 엔티티 생성
        SalesOrder order = new SalesOrder();
        // 비즈니스 로직...

        // 2. 저장
        SalesOrder savedOrder = salesOrderRepository.save(order);

        // 3. 도메인 이벤트 발행
        eventPublisher.publishEvent(new SalesOrderCreatedEvent(...));

        // 4. DTO 변환 후 반환
        return SalesOrderResponse.from(savedOrder);
    }
}
```

**특징**:

- `@Transactional(readOnly = true)`: 기본 읽기 전용 트랜잭션
- `@Transactional`: 쓰기 작업은 메서드 레벨에서 별도 선언
- 도메인 이벤트를 통한 모듈 간 통신

---

### 3. Repository Layer (Data Access)

**역할**: 데이터베이스 접근, CRUD 연산

**예시**: `SalesOrderRepository.java`

```java
@Repository
public interface SalesOrderRepository extends JpaRepository<SalesOrder, Long> {

    Optional<SalesOrder> findByIdAndDeletedFalse(Long id);

    @Query("SELECT so FROM SalesOrder so WHERE so.deleted = false ORDER BY so.createdAt DESC")
    Page<SalesOrder> findAllActive(Pageable pageable);

    long countByDeletedFalse();
}
```

**특징**:

- Spring Data JPA 사용
- Soft Delete 지원 (deleted 플래그)
- 메서드 네이밍 규칙으로 자동 쿼리 생성
- `@Query`를 통한 JPQL 작성

---

### 4. Entity Layer (Domain)

**역할**: 도메인 모델, 비즈니스 규칙 포함

**예시**: `SalesOrder.java`

```java
@Entity
@Table(name = "sales_orders")
public class SalesOrder extends BaseEntity {

    @Column(name = "order_number", unique = true)
    private String orderNumber;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    @OneToMany(mappedBy = "salesOrder", cascade = CascadeType.ALL)
    private List<SalesOrderLine> lines = new ArrayList<>();

    // 비즈니스 로직
    public void confirm() {
        if (this.status == OrderStatus.CONFIRMED) {
            throw new IllegalStateException("이미 확정된 수주입니다");
        }
        this.status = OrderStatus.CONFIRMED;
    }
}
```

**특징**:

- `BaseEntity` 상속 (ID, 생성일시, 수정일시, 삭제 플래그)
- JPA 어노테이션으로 매핑
- 도메인 로직 포함 (Aggregate Root)
- Cascade 옵션으로 자식 엔티티 관리

---

### 5. DTO Layer

**역할**: 계층 간 데이터 전송, API 스펙 정의

**Request DTO**: `SalesOrderRequest.java`

```java
public record SalesOrderRequest(
    @NotNull LocalDate orderDate,
    @NotBlank String customerCode,
    @NotBlank String customerName,
    String deliveryAddress,
    String remarks,
    @NotEmpty List<SalesOrderLineRequest> lines
) {
    public record SalesOrderLineRequest(
        @NotNull Integer lineNumber,
        @NotBlank String itemCode,
        @NotBlank String itemName,
        @NotNull BigDecimal quantity,
        @NotNull BigDecimal unitPrice,
        String remarks
    ) {}
}
```

**Response DTO**: `SalesOrderResponse.java`

```java
public record SalesOrderResponse(
    Long id,
    String orderNumber,
    LocalDate orderDate,
    String customerCode,
    String customerName,
    String status,
    BigDecimal totalAmount,
    String deliveryAddress,
    String remarks,
    List<SalesOrderLineResponse> lines,
    LocalDateTime createdAt,
    String createdBy
) {
    public static SalesOrderResponse from(SalesOrder order) {
        // Entity -> DTO 변환 로직
    }
}
```

---

### 6. Event Layer (Domain Events)

**역할**: 모듈 간 느슨한 결합, 비동기 통신

**이벤트 정의**: `SalesOrderCreatedEvent.java`

```java
public record SalesOrderCreatedEvent(
    Long salesOrderId,
    String orderNumber,
    List<OrderLineInfo> lines,
    LocalDateTime occurredAt
) {
    public record OrderLineInfo(
        String itemCode,
        BigDecimal quantity
    ) {}
}
```

**이벤트 리스너**: `SalesOrderEventListener.java` (inventory 패키지)

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class SalesOrderEventListener {

    private final StockService stockService;

    @EventListener
    @Async("taskExecutor")
    public void handleSalesOrderCreated(SalesOrderCreatedEvent event) {
        log.info("수주 생성 이벤트 수신 - 주문번호: {}", event.getOrderNumber());

        try {
            // 각 라인별로 재고 예약
            event.getLines().forEach(line -> {
                stockService.reserveStock(
                    line.getItemCode(),
                    "WH-001",  // 기본 창고
                    line.getQuantity()
                );
            });
            log.info("재고 예약 완료 - 주문번호: {}", event.getOrderNumber());
        } catch (Exception e) {
            log.error("재고 예약 실패: {}", e.getMessage());
            // 보상 트랜잭션 또는 알림 처리
        }
    }
}
```

---

## 🔄 데이터 흐름

### 수주 생성 플로우 예시

```
1. HTTP POST /api/v1/sales/orders
   └─> SalesOrderController.createSalesOrder()
       │
       ├─> [입력 검증] @Valid SalesOrderRequest
       │
       └─> SalesOrderService.createSalesOrder()
           │
           ├─> [엔티티 생성] new SalesOrder()
           ├─> [비즈니스 로직] order.addLine()
           ├─> [DB 저장] salesOrderRepository.save()
           ├─> [이벤트 발행] eventPublisher.publish(SalesOrderCreatedEvent)
           │   │
           │   └─> [비동기] SalesOrderEventListener.handleSalesOrderCreated()
           │       └─> stockService.reserveStock()
           │           └─> stockRepository.save()
           │
           └─> [DTO 변환] SalesOrderResponse.from()
               └─> [응답] ApiResponse.success()

2. HTTP 201 Created
   └─> JSON Response
```

### 트랜잭션 경계

```
┌─────────────────────────────────────────────┐
│  @Transactional (Service Layer)            │
│                                             │
│  1. salesOrderRepository.save()            │
│     └─> DB: INSERT sales_orders            │
│     └─> DB: INSERT sales_order_lines       │
│                                             │
│  2. eventPublisher.publish()               │
│     └─> 이벤트 큐에 등록                    │
│                                             │
│  [COMMIT]                                   │
└─────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────┐
│  @Async @EventListener (별도 트랜잭션)      │
│                                             │
│  SalesOrderEventListener                   │
│    .handleSalesOrderCreated()              │
│     └─> stockService.reserveStock()        │
│         └─> DB: UPDATE stocks              │
│                                             │
│  [COMMIT]                                   │
└─────────────────────────────────────────────┘
```

---

## 🎯 주요 기능 모듈

### 0. Auth Module (인증)

**기능**:

- 회원가입 (Signup)
- 로그인 (Login) - JWT 토큰 발급

**엔티티**:

- `User`: 사용자 정보

**역할(Role)**:

```
USER   - 일반 사용자
ADMIN  - 관리자
```

**관련 클래스**:

- `AuthController`: 인증 API 엔드포인트
- `AuthService`: 회원가입/로그인 비즈니스 로직
- `JwtTokenProvider`: JWT 토큰 생성 및 검증
- `JwtAuthenticationFilter`: 요청 시 JWT 토큰 검증

---

### 1. Sales Module (수주 관리)

**기능**:

- 수주 생성 (Create)
- 수주 조회 (Read) - 단건/목록
- 수주 수정 (Update)
- 수주 확정 (Confirm)
- 수주 삭제 (Soft Delete)

**엔티티**:

- `SalesOrder`: 수주 헤더
- `SalesOrderLine`: 수주 라인 (품목별 상세)

**상태 전이**:

```
PENDING → CONFIRMED → SHIPPED
   ↓
CANCELLED
```

**이벤트**:

- `SalesOrderCreatedEvent`: 재고 모듈에서 재고 예약 트리거

---

### 2. Inventory Module (재고 관리)

**기능**:

- 재고 조회 (품목별, 창고별)
- 재고 예약 (Reserve)
- 재고 차감 (Deduct)
- 재고 증가 (Increase)
- 재고 예약 해제 (Release)

**엔티티**:

- `Stock`: 재고 정보

**필드**:

```java
onHandQuantity      // 실재고
reservedQuantity    // 예약수량
availableQuantity   // 가용재고 = 실재고 - 예약수량
```

**이벤트 리스너**:

- `SalesOrderCreatedEvent` → 재고 예약

---

### 3. Accounting Module (회계 관리)

**기능**:

- 회계전표 생성 (Draft)
- 회계전표 전기 (Post)
- 회계전표 조회
- 회계전표 삭제

**엔티티**:

- `JournalEntry`: 회계전표 헤더
- `JournalEntryLine`: 전표 라인 (차변/대변)

**차대평형 검증**:

```java
public boolean isBalanced() {
    BigDecimal totalDebit = lines.stream()
        .map(JournalEntryLine::getDebit)
        .reduce(BigDecimal.ZERO, BigDecimal::add);

    BigDecimal totalCredit = lines.stream()
        .map(JournalEntryLine::getCredit)
        .reduce(BigDecimal.ZERO, BigDecimal::add);

    return totalDebit.compareTo(totalCredit) == 0;
}
```

**이벤트**:

- `JournalEntryPostedEvent`: 전표 전기 완료

---

### 4. OCR Module (문서 텍스트 추출)

**기능**:

- 이미지에서 텍스트 추출 (Amazon Textract DetectDocumentText)
- 테이블/폼 구조화 데이터 추출 (Amazon Textract AnalyzeDocument)

**지원 파일 형식**:

- PNG, JPG, JPEG, PDF

**API 엔드포인트**:

| API | 용도 | 설명 |
|-----|------|------|
| `POST /api/v1/ocr/extract` | 텍스트 추출 | 단순 텍스트 라인 추출 |
| `POST /api/v1/ocr/analyze` | 문서 분석 | 테이블, 폼 필드 구조화 추출 |

**구성 요소**:

- `AwsTextractClient`: AWS Textract API 호출 (detectDocumentText, analyzeDocument)
- `OcrService`: 파일 검증 및 응답 변환
- `OcrController`: REST API 엔드포인트

**텍스트 추출 응답 (OcrResponse)**:

```java
extractedText      // 추출된 전체 텍스트
blocks             // 블록별 상세 정보 (텍스트, 신뢰도, 타입)
averageConfidence  // 평균 신뢰도
```

**문서 분석 응답 (DocumentAnalysisResponse)**:

```java
extractedText      // 추출된 전체 텍스트
lines              // 텍스트 라인 목록
tables             // 테이블 목록 (cells, rows, headerToFirstRowMap 포함)
keyValuePairs      // 키-값 쌍 목록 (Order No: 528003-1322 형태)
formFields         // 키-값을 Map으로 제공 (편의용)
averageConfidence  // 평균 신뢰도
```

> 상세 문서: [docs/OCR_MODULE_GUIDE.md](docs/OCR_MODULE_GUIDE.md)

---

## 🛠️ 기술 스택

### Backend Framework

- **Spring Boot 3.x**: 애플리케이션 프레임워크
- **Spring Data JPA**: 데이터 접근 계층
- **Spring Web**: REST API
- **Spring Events**: 도메인 이벤트 처리
- **Spring Security**: JWT 토큰 기반 인증/인가

### Security

- **JJWT (Java JWT)**: JWT 토큰 생성 및 검증
- **BCrypt**: 비밀번호 암호화

### AWS

- **AWS SDK for Java 2.x**: AWS 서비스 연동
- **Amazon Textract**: 문서 텍스트 추출 (OCR)

### Database

- **PostgreSQL**: 관계형 데이터베이스
- **Hibernate**: JPA 구현체
- **HikariCP**: 커넥션 풀

### Validation & Serialization

- **Jakarta Bean Validation**: 입력 검증
- **Jackson**: JSON 직렬화/역직렬화

### Documentation

- **Springdoc OpenAPI (Swagger)**: API 문서 자동 생성

### Testing

- **JUnit 5**: 단위 테스트
- **MockMvc**: 컨트롤러 테스트
- **Mockito**: Mock 객체

### Build Tool

- **Gradle**: 빌드 도구

---

## 📊 데이터베이스 스키마

### 공통 필드 (BaseEntity)

모든 테이블에 공통으로 포함:

```sql
id BIGSERIAL PRIMARY KEY,
created_at TIMESTAMP NOT NULL,
updated_at TIMESTAMP,
created_by VARCHAR(50),
updated_by VARCHAR(50),
deleted BOOLEAN DEFAULT FALSE,
deleted_at TIMESTAMP
```

### users (사용자)

```sql
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(50) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    name VARCHAR(50) NOT NULL,
    role VARCHAR(20) NOT NULL DEFAULT 'USER',
    -- BaseEntity 필드
);
```

### sales_orders (수주)

```sql
CREATE TABLE sales_orders (
    id BIGSERIAL PRIMARY KEY,
    order_number VARCHAR(50) UNIQUE NOT NULL,
    order_date DATE NOT NULL,
    customer_code VARCHAR(50) NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    status VARCHAR(20) NOT NULL,
    total_amount DECIMAL(19,2) NOT NULL,
    delivery_address VARCHAR(500),
    remarks VARCHAR(1000),
    -- BaseEntity 필드
);
```

### sales_order_lines (수주 라인)

```sql
CREATE TABLE sales_order_lines (
    id BIGSERIAL PRIMARY KEY,
    sales_order_id BIGINT NOT NULL,
    line_number INTEGER NOT NULL,
    item_code VARCHAR(50) NOT NULL,
    item_name VARCHAR(200) NOT NULL,
    quantity DECIMAL(19,2) NOT NULL,
    unit_price DECIMAL(19,2) NOT NULL,
    line_amount DECIMAL(19,2) NOT NULL,
    remarks VARCHAR(500),
    -- BaseEntity 필드
    FOREIGN KEY (sales_order_id) REFERENCES sales_orders(id)
);
```

### stocks (재고)

```sql
CREATE TABLE stocks (
    id BIGSERIAL PRIMARY KEY,
    item_code VARCHAR(50) NOT NULL,
    item_name VARCHAR(200) NOT NULL,
    warehouse_code VARCHAR(50) NOT NULL,
    warehouse_name VARCHAR(200) NOT NULL,
    on_hand_quantity DECIMAL(19,2) NOT NULL,
    reserved_quantity DECIMAL(19,2) DEFAULT 0,
    available_quantity DECIMAL(19,2) NOT NULL,
    unit VARCHAR(20) NOT NULL,
    unit_price DECIMAL(19,2),
    -- BaseEntity 필드
    UNIQUE (item_code, warehouse_code)
);
```

### journal_entries (회계전표)

```sql
CREATE TABLE journal_entries (
    id BIGSERIAL PRIMARY KEY,
    entry_number VARCHAR(50) UNIQUE NOT NULL,
    entry_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL,
    description VARCHAR(1000),
    total_debit DECIMAL(19,2) NOT NULL,
    total_credit DECIMAL(19,2) NOT NULL,
    -- BaseEntity 필드
);
```

### journal_entry_lines (전표 라인)

```sql
CREATE TABLE journal_entry_lines (
    id BIGSERIAL PRIMARY KEY,
    journal_entry_id BIGINT NOT NULL,
    line_number INTEGER NOT NULL,
    account_code VARCHAR(50) NOT NULL,
    account_name VARCHAR(200) NOT NULL,
    debit DECIMAL(19,2) DEFAULT 0,
    credit DECIMAL(19,2) DEFAULT 0,
    description VARCHAR(500),
    -- BaseEntity 필드
    FOREIGN KEY (journal_entry_id) REFERENCES journal_entries(id)
);
```

---

## 🔐 보안 및 인증

### JWT 토큰 기반 인증

본 프로젝트는 Stateless JWT 토큰 기반 인증을 구현하고 있습니다.

**주요 특징**:

- **Stateless 인증**: 서버에 세션 저장 없이 토큰으로 인증
- **BCrypt 암호화**: 비밀번호 안전한 해싱
- **토큰 만료 시간**: 1시간 (3600000ms)
- **역할 기반 권한**: USER, ADMIN 역할 지원

### 인증 플로우

```
1. 로그인 요청 → AuthController → AuthService → JwtTokenProvider
2. JWT 토큰 발급 (userId + role 포함)
3. API 요청 시 Authorization 헤더에 토큰 포함
4. JwtAuthenticationFilter가 토큰 검증
5. SecurityContext에 인증 정보 저장
6. Controller 실행
```

### 엔드포인트 접근 권한

**인증 불필요 (PUBLIC)**:

- `/api/auth/**` (로그인, 회원가입)
- `/swagger-ui/**`, `/v3/api-docs/**`
- `/actuator/**`

**인증 필수**:

- `/api/v1/sales/**`
- `/api/v1/inventory/**`
- `/api/v1/accounting/**`
- 기타 모든 API

### SecurityConfig 설정

```java
@Configuration
@EnableWebSecurity
@RequiredArgsConstructor
public class SecurityConfig {

    private final JwtAuthenticationFilter jwtAuthenticationFilter;

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            .csrf(AbstractHttpConfigurer::disable)
            .cors(cors -> cors.configurationSource(corsConfigurationSource()))
            .authorizeHttpRequests(auth -> auth
                .requestMatchers(PUBLIC_URLS).permitAll()
                .anyRequest().authenticated())
            .sessionManagement(session -> session
                .sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .addFilterBefore(jwtAuthenticationFilter,
                UsernamePasswordAuthenticationFilter.class);

        return http.build();
    }
}
```

### JWT 설정 (application.yml)

```yaml
jwt:
  secret: ${JWT_SECRET:your-base64-encoded-secret-key}
  expiration: 3600000 # 1시간 (밀리초)
```

---

## 🧪 테스트 전략

### 1. Controller Layer Test

```java
@WebMvcTest(SalesOrderController.class)
class SalesOrderControllerTest {
    @Autowired MockMvc mockMvc;
    @MockitoBean SalesOrderService service;

    @Test
    void createSalesOrder_Success() throws Exception {
        // MockMvc를 통한 HTTP 요청/응답 테스트
    }
}
```

### 2. Service Layer Test

```java
@SpringBootTest
@Transactional
class SalesOrderServiceTest {
    @Autowired SalesOrderService service;
    @Autowired SalesOrderRepository repository;

    @Test
    void createSalesOrder_Success() {
        // 실제 DB 사용한 통합 테스트
    }
}
```

---

## 🚀 실행 방법

### 1. 데이터베이스 준비

```bash
# PostgreSQL 실행
docker run -d --name postgres -e POSTGRES_USER=kdh8281 -e POSTGRES_PASSWORD=8281 -e POSTGRES_DB=mydb -v pg_data:/var/lib/postgresql -p 5432:5432 postgres
```

### 2. 애플리케이션 실행

```bash
# Gradle 빌드
./gradlew clean build

# 애플리케이션 실행
./gradlew bootRun
```

### 3. API 문서 확인

```
http://localhost:8080/api/docs
```

---

## 📝 API 엔드포인트 예시

### Auth (인증)

```
POST   /api/auth/signup              # 회원가입
POST   /api/auth/login               # 로그인 (JWT 토큰 발급)
```

### Sales Orders

```
POST   /api/v1/sales/orders          # 수주 생성
GET    /api/v1/sales/orders          # 수주 목록 조회
GET    /api/v1/sales/orders/{id}     # 수주 상세 조회
PUT    /api/v1/sales/orders/{id}     # 수주 수정
DELETE /api/v1/sales/orders/{id}     # 수주 삭제
POST   /api/v1/sales/orders/{id}/confirm  # 수주 확정
```

### Stocks

```
POST   /api/v1/inventory/stocks                    # 재고 생성
GET    /api/v1/inventory/stocks                    # 전체 재고 조회
GET    /api/v1/inventory/stocks/{id}               # 재고 상세 조회
GET    /api/v1/inventory/stocks/item/{itemCode}    # 품목별 재고 조회
GET    /api/v1/inventory/stocks/warehouse/{code}   # 창고별 재고 조회
```

### Journal Entries

```
POST   /api/v1/accounting/journal-entries          # 전표 생성
GET    /api/v1/accounting/journal-entries          # 전표 목록 조회
GET    /api/v1/accounting/journal-entries/{id}     # 전표 상세 조회
POST   /api/v1/accounting/journal-entries/{id}/post  # 전표 전기
DELETE /api/v1/accounting/journal-entries/{id}     # 전표 삭제
```

### OCR

```bash
POST   /api/v1/ocr/extract              # 이미지에서 텍스트 추출
POST   /api/v1/ocr/analyze              # 문서 분석 (테이블/폼 구조화 추출)
```

---
