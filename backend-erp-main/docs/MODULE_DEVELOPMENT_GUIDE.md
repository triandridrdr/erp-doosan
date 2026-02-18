# API 모듈 개발 가이드

이 문서는 ERP API Template 프로젝트에 새로운 API 모듈을 추가하는 방법을 단계별로 설명합니다.

## 목차

1. [개요](#1-개요)
2. [모듈 생성 단계별 가이드](#2-모듈-생성-단계별-가이드)
3. [모듈 간 통신](#3-모듈-간-통신)
4. [체크리스트](#4-체크리스트)

---

## 1. 개요

### 1.1 모듈 아키텍처

이 프로젝트는 **Modular Monolith** 아키텍처를 따릅니다. 각 도메인(수주, 회계, 재고 등)은 독립적인 모듈로 구성되며, 모듈 간 통신은 **도메인 이벤트**를 통해 느슨하게 결합됩니다.

```
com/doosan/erp/
├── common/          # 공통 모듈 (BaseEntity, ApiResponse, ErrorCode 등)
├── config/          # 설정 클래스
├── security/        # 보안 (JWT)
├── auth/            # 인증 모듈
├── sales/           # 수주 모듈
├── accounting/      # 회계 모듈
├── inventory/       # 재고 모듈
├── ocr/             # OCR 모듈
└── purchase/        # 👈 새로 추가할 구매 모듈 (예제)
```

### 1.2 표준 모듈 구조

모든 모듈은 다음과 같은 표준 구조를 따릅니다:

```
{module}/
├── controller/      # REST API 엔드포인트
│   └── XxxController.java
├── service/         # 비즈니스 로직
│   └── XxxService.java
├── repository/      # 데이터 접근
│   └── XxxRepository.java
├── entity/          # JPA 엔티티
│   └── Xxx.java
├── dto/             # 요청/응답 DTO
│   ├── XxxRequest.java
│   └── XxxResponse.java
├── event/           # 도메인 이벤트 (선택)
│   └── XxxCreatedEvent.java
└── listener/        # 이벤트 리스너 (선택)
    └── XxxEventListener.java
```

---

## 2. 모듈 생성 단계별 가이드

**예제: 구매(Purchase) 모듈 생성**

구매 모듈은 공급업체로부터 자재를 구매하는 발주 관리 기능을 담당합니다.

### Step 1: 패키지 구조 생성

다음 디렉토리 구조를 생성합니다:

```
src/main/java/com/doosan/erp/purchase/
├── controller/
├── service/
├── repository/
├── entity/
├── dto/
├── event/      # 이벤트 사용 시
└── listener/   # 이벤트 수신 시
```

---

### Step 2: Entity 생성

`BaseEntity`를 상속하여 엔티티를 생성합니다. `BaseEntity`는 다음 기능을 제공합니다:
- `id`: 자동 증가 기본키
- `createdAt`, `updatedAt`: 생성/수정 시간 자동 기록
- `createdBy`, `updatedBy`: 생성/수정자 자동 기록
- `deleted`, `deletedAt`: Soft Delete 지원

**PurchaseOrder.java (발주 헤더)**

```java
package com.doosan.erp.purchase.entity;

import com.doosan.erp.common.entity.BaseEntity;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "purchase_orders")
@Getter
@Setter
@NoArgsConstructor
public class PurchaseOrder extends BaseEntity {

    // 발주번호: 시스템에서 자동 생성 (예: PO-2025-0001)
    @Column(name = "order_number", unique = true, nullable = false, length = 50)
    private String orderNumber;

    // 발주일자
    @Column(name = "order_date", nullable = false)
    private LocalDate orderDate;

    // 공급업체 코드
    @Column(name = "supplier_code", nullable = false, length = 50)
    private String supplierCode;

    // 공급업체명
    @Column(name = "supplier_name", nullable = false, length = 200)
    private String supplierName;

    // 발주 상태
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private OrderStatus status = OrderStatus.DRAFT;

    // 총 금액 (자동 계산)
    @Column(name = "total_amount", precision = 19, scale = 2, nullable = false)
    private BigDecimal totalAmount = BigDecimal.ZERO;

    // 비고
    @Column(name = "remarks", length = 1000)
    private String remarks;

    // 발주 라인 목록 (1:N 관계)
    @OneToMany(mappedBy = "purchaseOrder", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    private List<PurchaseOrderLine> lines = new ArrayList<>();

    /**
     * 발주 상태 열거형
     */
    public enum OrderStatus {
        DRAFT,      // 작성중
        SUBMITTED,  // 제출됨
        APPROVED,   // 승인됨
        RECEIVED,   // 입고완료
        CANCELLED   // 취소
    }

    /**
     * 발주 라인 추가 (양방향 관계 설정)
     */
    public void addLine(PurchaseOrderLine line) {
        lines.add(line);
        line.setPurchaseOrder(this);
        recalculateTotalAmount();
    }

    /**
     * 발주 라인 제거
     */
    public void removeLine(PurchaseOrderLine line) {
        lines.remove(line);
        line.setPurchaseOrder(null);
        recalculateTotalAmount();
    }

    /**
     * 총 금액 재계산
     */
    public void recalculateTotalAmount() {
        this.totalAmount = lines.stream()
                .map(PurchaseOrderLine::getLineAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    /**
     * 발주 승인
     */
    public void approve() {
        if (this.status != OrderStatus.SUBMITTED) {
            throw new IllegalStateException("제출된 발주만 승인할 수 있습니다");
        }
        this.status = OrderStatus.APPROVED;
    }

    /**
     * 발주 취소
     */
    public void cancel() {
        if (this.status == OrderStatus.RECEIVED) {
            throw new IllegalStateException("입고완료된 발주는 취소할 수 없습니다");
        }
        this.status = OrderStatus.CANCELLED;
    }
}
```

**PurchaseOrderLine.java (발주 라인)**

```java
package com.doosan.erp.purchase.entity;

import com.doosan.erp.common.entity.BaseEntity;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

@Entity
@Table(name = "purchase_order_lines")
@Getter
@Setter
@NoArgsConstructor
public class PurchaseOrderLine extends BaseEntity {

    // 발주 헤더와의 N:1 관계
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "purchase_order_id", nullable = false)
    private PurchaseOrder purchaseOrder;

    // 라인 번호
    @Column(name = "line_number", nullable = false)
    private Integer lineNumber;

    // 품목코드
    @Column(name = "item_code", nullable = false, length = 50)
    private String itemCode;

    // 품목명
    @Column(name = "item_name", nullable = false, length = 200)
    private String itemName;

    // 수량
    @Column(name = "quantity", precision = 19, scale = 2, nullable = false)
    private BigDecimal quantity;

    // 단가
    @Column(name = "unit_price", precision = 19, scale = 2, nullable = false)
    private BigDecimal unitPrice;

    // 라인 금액 (자동 계산: 수량 * 단가)
    @Column(name = "line_amount", precision = 19, scale = 2, nullable = false)
    private BigDecimal lineAmount = BigDecimal.ZERO;

    // 비고
    @Column(name = "remarks", length = 500)
    private String remarks;

    /**
     * 생성자 (라인 금액 자동 계산)
     */
    public PurchaseOrderLine(Integer lineNumber, String itemCode, String itemName,
                              BigDecimal quantity, BigDecimal unitPrice, String remarks) {
        this.lineNumber = lineNumber;
        this.itemCode = itemCode;
        this.itemName = itemName;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.remarks = remarks;
        recalculateLineAmount();
    }

    /**
     * 라인 금액 재계산
     */
    public void recalculateLineAmount() {
        if (this.quantity != null && this.unitPrice != null) {
            this.lineAmount = this.quantity.multiply(this.unitPrice);
        }
    }
}
```

---

### Step 3: Repository 생성

`JpaRepository`를 상속하여 Repository를 생성합니다. **Soft Delete** 패턴을 적용하여 모든 조회에서 `deleted=false` 조건을 사용합니다.

**PurchaseOrderRepository.java**

```java
package com.doosan.erp.purchase.repository;

import com.doosan.erp.purchase.entity.PurchaseOrder;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface PurchaseOrderRepository extends JpaRepository<PurchaseOrder, Long> {

    /**
     * ID로 삭제되지 않은 발주 조회
     */
    Optional<PurchaseOrder> findByIdAndDeletedFalse(Long id);

    /**
     * 발주번호로 조회
     */
    Optional<PurchaseOrder> findByOrderNumberAndDeletedFalse(String orderNumber);

    /**
     * 활성 발주 목록 조회 (페이징)
     */
    @Query("SELECT po FROM PurchaseOrder po WHERE po.deleted = false ORDER BY po.createdAt DESC")
    Page<PurchaseOrder> findAllActive(Pageable pageable);

    /**
     * 활성 발주 개수 조회
     */
    long countByDeletedFalse();

    /**
     * 특정 연도의 최대 순번 조회 (발주번호 생성용)
     */
    @Query(value = "SELECT COALESCE(MAX(CAST(SUBSTRING_INDEX(order_number, '-', -1) AS UNSIGNED)), 0) " +
            "FROM purchase_orders " +
            "WHERE order_number LIKE :yearPattern AND deleted = false", nativeQuery = true)
    Integer findMaxSequenceByYear(@Param("yearPattern") String yearPattern);
}
```

---

### Step 4: DTO 생성

**Request DTO**와 **Response DTO**를 분리하여 생성합니다.

#### Request DTO

클라이언트 입력값 검증을 위해 **Jakarta Bean Validation** 어노테이션을 사용합니다.

**PurchaseOrderRequest.java**

```java
package com.doosan.erp.purchase.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PurchaseOrderRequest {

    @NotNull(message = "발주일자는 필수입니다")
    private LocalDate orderDate;

    @NotBlank(message = "공급업체 코드는 필수입니다")
    private String supplierCode;

    @NotBlank(message = "공급업체명은 필수입니다")
    private String supplierName;

    private String remarks;

    @NotEmpty(message = "발주 라인은 최소 1개 이상 필요합니다")
    @Valid  // 중첩 객체 검증
    private List<PurchaseOrderLineRequest> lines;

    /**
     * 발주 라인 요청 DTO
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PurchaseOrderLineRequest {

        @NotNull(message = "라인번호는 필수입니다")
        private Integer lineNumber;

        @NotBlank(message = "품목코드는 필수입니다")
        private String itemCode;

        @NotBlank(message = "품목명은 필수입니다")
        private String itemName;

        @NotNull(message = "수량은 필수입니다")
        private BigDecimal quantity;

        @NotNull(message = "단가는 필수입니다")
        private BigDecimal unitPrice;

        private String remarks;
    }
}
```

#### Response DTO

Entity를 DTO로 변환하는 `from()` 정적 팩토리 메서드를 제공합니다.

**PurchaseOrderResponse.java**

```java
package com.doosan.erp.purchase.dto;

import com.doosan.erp.purchase.entity.PurchaseOrder;
import com.doosan.erp.purchase.entity.PurchaseOrderLine;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PurchaseOrderResponse {

    private Long id;
    private String orderNumber;
    private LocalDate orderDate;
    private String supplierCode;
    private String supplierName;
    private String status;
    private BigDecimal totalAmount;
    private String remarks;
    private List<PurchaseOrderLineResponse> lines;
    private LocalDateTime createdAt;
    private String createdBy;

    /**
     * Entity → DTO 변환 팩토리 메서드
     */
    public static PurchaseOrderResponse from(PurchaseOrder entity) {
        return new PurchaseOrderResponse(
                entity.getId(),
                entity.getOrderNumber(),
                entity.getOrderDate(),
                entity.getSupplierCode(),
                entity.getSupplierName(),
                entity.getStatus().name(),
                entity.getTotalAmount(),
                entity.getRemarks(),
                entity.getLines().stream()
                        .map(PurchaseOrderLineResponse::from)
                        .collect(Collectors.toList()),
                entity.getCreatedAt(),
                entity.getCreatedBy()
        );
    }

    /**
     * 발주 라인 응답 DTO
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PurchaseOrderLineResponse {
        private Long id;
        private Integer lineNumber;
        private String itemCode;
        private String itemName;
        private BigDecimal quantity;
        private BigDecimal unitPrice;
        private BigDecimal lineAmount;
        private String remarks;

        public static PurchaseOrderLineResponse from(PurchaseOrderLine entity) {
            return new PurchaseOrderLineResponse(
                    entity.getId(),
                    entity.getLineNumber(),
                    entity.getItemCode(),
                    entity.getItemName(),
                    entity.getQuantity(),
                    entity.getUnitPrice(),
                    entity.getLineAmount(),
                    entity.getRemarks()
            );
        }
    }
}
```

---

### Step 5: Service 생성

비즈니스 로직을 처리하는 Service를 생성합니다.

**PurchaseOrderService.java**

```java
package com.doosan.erp.purchase.service;

import com.doosan.erp.common.constant.ErrorCode;
import com.doosan.erp.common.dto.PageResponse;
import com.doosan.erp.common.exception.BusinessException;
import com.doosan.erp.common.exception.ResourceNotFoundException;
import com.doosan.erp.purchase.dto.PurchaseOrderRequest;
import com.doosan.erp.purchase.dto.PurchaseOrderResponse;
import com.doosan.erp.purchase.entity.PurchaseOrder;
import com.doosan.erp.purchase.entity.PurchaseOrderLine;
import com.doosan.erp.purchase.repository.PurchaseOrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)  // 기본 읽기 전용
public class PurchaseOrderService {

    private final PurchaseOrderRepository purchaseOrderRepository;
    private final ApplicationEventPublisher eventPublisher;  // 이벤트 발행용

    /**
     * 발주 생성
     */
    @Transactional
    public PurchaseOrderResponse createPurchaseOrder(PurchaseOrderRequest request) {
        log.info("Creating purchase order for supplier: {}", request.getSupplierCode());

        // 발주 엔티티 생성
        PurchaseOrder order = new PurchaseOrder();
        order.setOrderNumber(generateOrderNumber());
        order.setOrderDate(request.getOrderDate());
        order.setSupplierCode(request.getSupplierCode());
        order.setSupplierName(request.getSupplierName());
        order.setRemarks(request.getRemarks());
        order.setStatus(PurchaseOrder.OrderStatus.DRAFT);

        // 발주 라인 추가
        request.getLines().forEach(lineReq -> {
            PurchaseOrderLine line = new PurchaseOrderLine(
                    lineReq.getLineNumber(),
                    lineReq.getItemCode(),
                    lineReq.getItemName(),
                    lineReq.getQuantity(),
                    lineReq.getUnitPrice(),
                    lineReq.getRemarks()
            );
            order.addLine(line);
        });

        PurchaseOrder savedOrder = purchaseOrderRepository.save(order);
        log.info("Purchase order created: {}", savedOrder.getOrderNumber());

        return PurchaseOrderResponse.from(savedOrder);
    }

    /**
     * 발주 단건 조회
     */
    public PurchaseOrderResponse getPurchaseOrder(Long id) {
        log.info("Getting purchase order: {}", id);

        PurchaseOrder order = purchaseOrderRepository.findByIdAndDeletedFalse(id)
                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.PURCHASE_ORDER_NOT_FOUND));

        return PurchaseOrderResponse.from(order);
    }

    /**
     * 발주 목록 조회 (페이징)
     */
    public PageResponse<PurchaseOrderResponse> getPurchaseOrders(int page, int size) {
        log.info("Getting purchase orders - page: {}, size: {}", page, size);

        Pageable pageable = PageRequest.of(page, size);
        Page<PurchaseOrder> orderPage = purchaseOrderRepository.findAllActive(pageable);

        List<PurchaseOrderResponse> responses = orderPage.getContent().stream()
                .map(PurchaseOrderResponse::from)
                .collect(Collectors.toList());

        return PageResponse.of(responses, page, size, orderPage.getTotalElements());
    }

    /**
     * 발주 수정
     */
    @Transactional
    public PurchaseOrderResponse updatePurchaseOrder(Long id, PurchaseOrderRequest request) {
        log.info("Updating purchase order: {}", id);

        PurchaseOrder order = purchaseOrderRepository.findByIdAndDeletedFalse(id)
                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.PURCHASE_ORDER_NOT_FOUND));

        // 비즈니스 규칙: 승인된 발주는 수정 불가
        if (order.getStatus() == PurchaseOrder.OrderStatus.APPROVED) {
            throw new BusinessException(ErrorCode.PURCHASE_ORDER_ALREADY_APPROVED,
                    "승인된 발주는 수정할 수 없습니다");
        }

        // 헤더 정보 수정
        order.setOrderDate(request.getOrderDate());
        order.setSupplierCode(request.getSupplierCode());
        order.setSupplierName(request.getSupplierName());
        order.setRemarks(request.getRemarks());

        // 라인 재설정
        order.getLines().clear();
        request.getLines().forEach(lineReq -> {
            PurchaseOrderLine line = new PurchaseOrderLine(
                    lineReq.getLineNumber(),
                    lineReq.getItemCode(),
                    lineReq.getItemName(),
                    lineReq.getQuantity(),
                    lineReq.getUnitPrice(),
                    lineReq.getRemarks()
            );
            order.addLine(line);
        });

        PurchaseOrder updatedOrder = purchaseOrderRepository.save(order);
        log.info("Purchase order updated: {}", updatedOrder.getOrderNumber());

        return PurchaseOrderResponse.from(updatedOrder);
    }

    /**
     * 발주 승인
     */
    @Transactional
    public PurchaseOrderResponse approvePurchaseOrder(Long id) {
        log.info("Approving purchase order: {}", id);

        PurchaseOrder order = purchaseOrderRepository.findByIdAndDeletedFalse(id)
                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.PURCHASE_ORDER_NOT_FOUND));

        order.approve();
        PurchaseOrder approvedOrder = purchaseOrderRepository.save(order);

        // 이벤트 발행 (재고 입고 트리거) - Step 7 참조
        // eventPublisher.publishEvent(new PurchaseOrderApprovedEvent(...));

        log.info("Purchase order approved: {}", approvedOrder.getOrderNumber());

        return PurchaseOrderResponse.from(approvedOrder);
    }

    /**
     * 발주 삭제 (Soft Delete)
     */
    @Transactional
    public void deletePurchaseOrder(Long id) {
        log.info("Deleting purchase order: {}", id);

        PurchaseOrder order = purchaseOrderRepository.findByIdAndDeletedFalse(id)
                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.PURCHASE_ORDER_NOT_FOUND));

        // 비즈니스 규칙: 입고완료된 발주는 삭제 불가
        if (order.getStatus() == PurchaseOrder.OrderStatus.RECEIVED) {
            throw new BusinessException(ErrorCode.INVALID_PURCHASE_ORDER,
                    "입고완료된 발주는 삭제할 수 없습니다");
        }

        order.softDelete();
        purchaseOrderRepository.save(order);

        log.info("Purchase order deleted: {}", order.getOrderNumber());
    }

    /**
     * 발주번호 자동 생성
     * 형식: PO-{연도}-{순번} (예: PO-2025-0001)
     */
    private String generateOrderNumber() {
        int currentYear = LocalDateTime.now().getYear();
        String yearPattern = String.format("PO-%04d-%%", currentYear);

        Integer maxSequence = purchaseOrderRepository.findMaxSequenceByYear(yearPattern);
        if (maxSequence == null) {
            maxSequence = 0;
        }

        int nextSequence = maxSequence + 1;
        return String.format("PO-%04d-%04d", currentYear, nextSequence);
    }
}
```

---

### Step 6: Controller 생성

REST API 엔드포인트를 제공하는 Controller를 생성합니다.

**PurchaseOrderController.java**

```java
package com.doosan.erp.purchase.controller;

import com.doosan.erp.common.dto.ApiResponse;
import com.doosan.erp.common.dto.PageResponse;
import com.doosan.erp.purchase.dto.PurchaseOrderRequest;
import com.doosan.erp.purchase.dto.PurchaseOrderResponse;
import com.doosan.erp.purchase.service.PurchaseOrderService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/purchase/orders")
@RequiredArgsConstructor
@Tag(name = "Purchase - Order", description = "발주 관리 API")
public class PurchaseOrderController {

    private final PurchaseOrderService purchaseOrderService;

    /**
     * 발주 생성
     */
    @PostMapping
    @Operation(summary = "발주 생성", description = "새로운 발주를 생성합니다")
    public ResponseEntity<ApiResponse<PurchaseOrderResponse>> createPurchaseOrder(
            @Valid @RequestBody PurchaseOrderRequest request) {
        PurchaseOrderResponse response = purchaseOrderService.createPurchaseOrder(request);
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(ApiResponse.success(response, "발주가 생성되었습니다"));
    }

    /**
     * 발주 단건 조회
     */
    @GetMapping("/{id}")
    @Operation(summary = "발주 조회", description = "발주 상세 정보를 조회합니다")
    public ResponseEntity<ApiResponse<PurchaseOrderResponse>> getPurchaseOrder(@PathVariable Long id) {
        PurchaseOrderResponse response = purchaseOrderService.getPurchaseOrder(id);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    /**
     * 발주 목록 조회 (페이징)
     */
    @GetMapping
    @Operation(summary = "발주 목록 조회", description = "발주 목록을 페이징하여 조회합니다")
    public ResponseEntity<ApiResponse<PageResponse<PurchaseOrderResponse>>> getPurchaseOrders(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        PageResponse<PurchaseOrderResponse> response = purchaseOrderService.getPurchaseOrders(page, size);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    /**
     * 발주 수정
     */
    @PutMapping("/{id}")
    @Operation(summary = "발주 수정", description = "발주 정보를 수정합니다")
    public ResponseEntity<ApiResponse<PurchaseOrderResponse>> updatePurchaseOrder(
            @PathVariable Long id,
            @Valid @RequestBody PurchaseOrderRequest request) {
        PurchaseOrderResponse response = purchaseOrderService.updatePurchaseOrder(id, request);
        return ResponseEntity.ok(ApiResponse.success(response, "발주가 수정되었습니다"));
    }

    /**
     * 발주 승인
     */
    @PostMapping("/{id}/approve")
    @Operation(summary = "발주 승인", description = "발주를 승인 처리합니다")
    public ResponseEntity<ApiResponse<PurchaseOrderResponse>> approvePurchaseOrder(@PathVariable Long id) {
        PurchaseOrderResponse response = purchaseOrderService.approvePurchaseOrder(id);
        return ResponseEntity.ok(ApiResponse.success(response, "발주가 승인되었습니다"));
    }

    /**
     * 발주 삭제 (Soft Delete)
     */
    @DeleteMapping("/{id}")
    @Operation(summary = "발주 삭제", description = "발주를 삭제합니다 (Soft Delete)")
    public ResponseEntity<ApiResponse<Void>> deletePurchaseOrder(@PathVariable Long id) {
        purchaseOrderService.deletePurchaseOrder(id);
        return ResponseEntity.ok(ApiResponse.success(null, "발주가 삭제되었습니다"));
    }
}
```

---

### Step 7: ErrorCode 추가

`ErrorCode.java`에 새 모듈의 에러 코드를 추가합니다.

**에러 코드 체계:**
- 1000번대: 공통 에러
- 1100번대: 인증 도메인 에러
- 2000번대: 회계 도메인 에러
- 3000번대: 판매 도메인 에러
- 4000번대: 재고 도메인 에러
- 5000번대: OCR 도메인 에러
- **6000번대: 구매 도메인 에러 (신규)**

**ErrorCode.java에 추가할 코드:**

```java
// ==================== 구매 도메인 에러 (6000번대) ====================
PURCHASE_ORDER_NOT_FOUND("ERR-6001", "발주를 찾을 수 없습니다", HttpStatus.NOT_FOUND),
PURCHASE_ORDER_ALREADY_APPROVED("ERR-6002", "이미 승인된 발주입니다", HttpStatus.BAD_REQUEST),
INVALID_PURCHASE_ORDER("ERR-6003", "유효하지 않은 발주입니다", HttpStatus.BAD_REQUEST),
```

---

## 3. 모듈 간 통신

모듈 간 통신이 필요한 경우 두 가지 방법을 사용할 수 있습니다.

### 방법 1: 이벤트 기반 통신 (권장)

**장점:**
- 모듈 간 **느슨한 결합** 유지
- **확장성**: 새로운 리스너 추가 시 기존 코드 수정 불필요
- **비동기 처리**: 메인 트랜잭션과 분리하여 성능 향상
- **장애 격리**: 리스너 실패가 발행 모듈에 영향 없음

**예제: 발주 승인 시 재고 입고 처리**

#### 1) 이벤트 클래스 생성

```java
package com.doosan.erp.purchase.event;

import com.doosan.erp.common.event.DomainEvent;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Getter
@AllArgsConstructor
public class PurchaseOrderApprovedEvent implements DomainEvent {

    private final Long orderId;
    private final String orderNumber;
    private final List<OrderLineInfo> lines;
    private final LocalDateTime occurredAt;

    @Getter
    @AllArgsConstructor
    public static class OrderLineInfo {
        private final String itemCode;
        private final BigDecimal quantity;
    }
}
```

#### 2) Service에서 이벤트 발행

```java
@Transactional
public PurchaseOrderResponse approvePurchaseOrder(Long id) {
    PurchaseOrder order = purchaseOrderRepository.findByIdAndDeletedFalse(id)
            .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.PURCHASE_ORDER_NOT_FOUND));

    order.approve();
    PurchaseOrder approvedOrder = purchaseOrderRepository.save(order);

    // 이벤트 발행
    List<PurchaseOrderApprovedEvent.OrderLineInfo> lineInfos = approvedOrder.getLines().stream()
            .map(line -> new PurchaseOrderApprovedEvent.OrderLineInfo(
                    line.getItemCode(),
                    line.getQuantity()))
            .collect(Collectors.toList());

    PurchaseOrderApprovedEvent event = new PurchaseOrderApprovedEvent(
            approvedOrder.getId(),
            approvedOrder.getOrderNumber(),
            lineInfos,
            LocalDateTime.now());
    eventPublisher.publishEvent(event);  // 이벤트 발행

    return PurchaseOrderResponse.from(approvedOrder);
}
```

#### 3) 기존 리스너에 이벤트 핸들러 추가

기존 `SalesOrderEventListener`에 새로운 이벤트 핸들러 메서드를 추가합니다.

**inventory/listener/SalesOrderEventListener.java** (기존 파일에 메서드 추가)

```java
package com.doosan.erp.inventory.listener;

import com.doosan.erp.inventory.service.StockService;
import com.doosan.erp.purchase.event.PurchaseOrderApprovedEvent;  // 새로 추가
import com.doosan.erp.sales.event.SalesOrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SalesOrderEventListener {

    private final StockService stockService;

    // 기존 메서드: 수주 생성 시 재고 예약
    @EventListener
    @Async("taskExecutor")
    public void handleSalesOrderCreated(SalesOrderCreatedEvent event) {
        // ... 기존 코드 ...
    }

    /**
     * 발주 승인 이벤트 처리 (새로 추가)
     *
     * 발주가 승인되면 해당 품목의 재고를 입고 처리합니다.
     */
    @EventListener
    @Async("taskExecutor")
    public void handlePurchaseOrderApproved(PurchaseOrderApprovedEvent event) {
        log.info("발주 승인 이벤트 수신 - 발주번호: {}", event.getOrderNumber());

        try {
            event.getLines().forEach(line -> {
                log.info("재고 입고 처리 - 품목: {}, 수량: {}",
                        line.getItemCode(), line.getQuantity());

                String defaultWarehouse = "WH-001";
                stockService.receiveStock(
                        line.getItemCode(),
                        defaultWarehouse,
                        line.getQuantity()
                );
            });

            log.info("재고 입고 완료 - 발주번호: {}", event.getOrderNumber());

        } catch (Exception e) {
            log.error("재고 입고 실패 - 발주번호: {}, 에러: {}",
                    event.getOrderNumber(), e.getMessage(), e);
            // 실제 운영 환경에서는 보상 트랜잭션 또는 알림 처리
        }
    }
}
```

> **참고**: 리스너 클래스명을 `SalesOrderEventListener`에서 `InventoryEventListener`와 같이 더 일반적인 이름으로 변경하는 것도 고려해볼 수 있습니다.

---

### 방법 2: 직접 의존성 주입 (대안)

이벤트 방식이 복잡하게 느껴지거나, 간단한 조회만 필요한 경우 다른 모듈의 Repository나 Service를 직접 주입받아 사용할 수 있습니다.

**예제: 발주 생성 시 품목 정보 조회**

```java
package com.doosan.erp.purchase.service;

import com.doosan.erp.inventory.repository.StockRepository;  // 다른 모듈 Repository 주입
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PurchaseOrderService {

    private final PurchaseOrderRepository purchaseOrderRepository;
    private final StockRepository stockRepository;  // 재고 모듈의 Repository 직접 주입

    @Transactional
    public PurchaseOrderResponse createPurchaseOrder(PurchaseOrderRequest request) {
        // 품목 존재 여부 확인 (다른 모듈 Repository 직접 사용)
        request.getLines().forEach(line -> {
            boolean exists = stockRepository.existsByItemCodeAndDeletedFalse(line.getItemCode());
            if (!exists) {
                throw new BusinessException(ErrorCode.ITEM_NOT_FOUND,
                        "품목을 찾을 수 없습니다: " + line.getItemCode());
            }
        });

        // 발주 생성 로직...
    }
}
```

**주의사항:**
- 모듈 간 **결합도가 증가**합니다
- 나중에 모듈을 분리(마이크로서비스화)할 때 수정이 필요합니다
- 간단한 조회 용도로만 사용하고, 복잡한 비즈니스 로직은 **이벤트 방식을 권장**합니다

---

## 4. 체크리스트

새 모듈 추가 시 다음 항목을 확인하세요:

### 필수 항목

- [ ] 패키지 구조 생성 (`controller`, `service`, `repository`, `entity`, `dto`)
- [ ] Entity 클래스 생성 (`BaseEntity` 상속)
- [ ] Repository 인터페이스 생성 (`JpaRepository` 상속)
- [ ] Request DTO 생성 (Validation 어노테이션 적용)
- [ ] Response DTO 생성 (`from()` 팩토리 메서드)
- [ ] Service 클래스 생성 (`@Transactional` 적용)
- [ ] Controller 클래스 생성 (`ApiResponse` 래핑, Swagger 어노테이션)
- [ ] ErrorCode 추가 (도메인별 에러 코드 체계)

### 선택 항목

- [ ] 도메인 이벤트 클래스 생성 (`DomainEvent` 구현)
- [ ] 이벤트 리스너 생성 (`@EventListener`, `@Async`)
- [ ] 단위 테스트 작성
- [ ] 통합 테스트 작성
- [ ] API_SAMPLES.md에 요청/응답 예시 추가

### 코드 품질

- [ ] 모든 필드에 적절한 Validation 적용
- [ ] Soft Delete 패턴 적용 (`deleted=false` 조건)
- [ ] 비즈니스 규칙 검증 로직 추가
- [ ] 적절한 로깅 추가 (`@Slf4j`)
- [ ] Swagger 문서화 (`@Tag`, `@Operation`)

---

## 참고 자료

- 기존 모듈 참조: `sales/`, `accounting/`, `inventory/`
- 공통 모듈: `common/entity/BaseEntity.java`, `common/dto/ApiResponse.java`
- API 샘플: `docs/API_SAMPLES.md`
