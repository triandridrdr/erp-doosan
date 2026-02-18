package com.doosan.erp.sales.service;

import com.doosan.erp.common.constant.ErrorCode;
import com.doosan.erp.common.dto.PageResponse;
import com.doosan.erp.common.exception.BusinessException;
import com.doosan.erp.common.exception.ResourceNotFoundException;
import com.doosan.erp.sales.dto.SalesOrderRequest;
import com.doosan.erp.sales.dto.SalesOrderResponse;
import com.doosan.erp.sales.entity.SalesOrder;
import com.doosan.erp.sales.entity.SalesOrderLine;
import com.doosan.erp.sales.event.SalesOrderCreatedEvent;
import com.doosan.erp.sales.repository.SalesOrderRepository;
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

/**
 * 수주 서비스
 *
 * 수주(Sales Order) 관련 비즈니스 로직을 처리하는 서비스 클래스입니다.
 * 트랜잭션 관리, 비즈니스 규칙 검증, 도메인 이벤트 발행 등을 담당합니다.
 *
 * 주요 기능:
 * - 수주 생성: 주문번호 자동 생성, 라인 추가, 이벤트 발행
 * - 수주 조회: 단건/목록 조회 (페이징 지원)
 * - 수주 수정: 확정 전 수주만 수정 가능
 * - 수주 확정: 대기 상태를 확정으로 변경
 * - 수주 삭제: Soft Delete (출하완료 건 삭제 불가)
 *
 * Transactional(readOnly = true): 기본적으로 읽기 전용 트랜잭션 적용
 * 쓰기 작업 메서드에는 별도로 Transactional 어노테이션 적용
 */
@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class SalesOrderService {

        // 수주 데이터베이스 접근을 위한 Repository
        private final SalesOrderRepository salesOrderRepository;

        // 도메인 이벤트 발행을 위한 Spring의 이벤트 퍼블리셔
        private final ApplicationEventPublisher eventPublisher;

        /**
         * 수주 생성
         *
         * 새로운 수주를 생성합니다.
         * 처리 순서:
         * 1. 수주 엔티티 생성 및 기본 정보 설정
         * 2. 수주 라인(상품 목록) 추가
         * 3. 데이터베이스 저장
         * 4. 수주 생성 이벤트 발행 (재고 모듈에서 수신하여 재고 예약 처리)
         *
         * @param request 수주 생성 요청 정보
         * @return 생성된 수주 정보
         */
        @Transactional
        public SalesOrderResponse createSalesOrder(SalesOrderRequest request) {
                log.info("Creating sales order for customer: {}", request.getCustomerCode());

                // 수주 엔티티 생성 및 기본 정보 설정
                SalesOrder order = new SalesOrder();
                order.setOrderNumber(generateOrderNumber());
                order.setOrderDate(request.getOrderDate());
                order.setCustomerCode(request.getCustomerCode());
                order.setCustomerName(request.getCustomerName());
                order.setDeliveryAddress(request.getDeliveryAddress());
                order.setRemarks(request.getRemarks());
                order.setStatus(SalesOrder.OrderStatus.PENDING);

                // 수주 라인 추가 (품목별 주문 상세 정보)
                request.getLines().forEach(lineReq -> {
                        SalesOrderLine line = new SalesOrderLine(
                                        lineReq.getLineNumber(),
                                        lineReq.getItemCode(),
                                        lineReq.getItemName(),
                                        lineReq.getQuantity(),
                                        lineReq.getUnitPrice(),
                                        lineReq.getRemarks());
                        order.addLine(line);
                });

                // 데이터베이스에 수주 저장
                SalesOrder savedOrder = salesOrderRepository.save(order);
                log.info("Sales order created: {}", savedOrder.getOrderNumber());

                // 수주 생성 이벤트 발행 (다른 모듈에서 수신하여 후속 처리 수행)
                List<SalesOrderCreatedEvent.OrderLineInfo> lineInfos = savedOrder.getLines().stream()
                                .map(line -> new SalesOrderCreatedEvent.OrderLineInfo(
                                                line.getItemCode(),
                                                line.getQuantity()))
                                .collect(Collectors.toList());

                SalesOrderCreatedEvent event = new SalesOrderCreatedEvent(
                                savedOrder.getId(),
                                savedOrder.getOrderNumber(),
                                lineInfos,
                                LocalDateTime.now());
                eventPublisher.publishEvent(event);

                return SalesOrderResponse.from(savedOrder);
        }

        /**
         * 수주 단건 조회
         *
         * ID로 수주를 조회합니다. 삭제된 수주는 조회되지 않습니다.
         *
         * @param id 수주 ID
         * @return 수주 상세 정보
         * @throws ResourceNotFoundException 수주를 찾을 수 없는 경우
         */
        public SalesOrderResponse getSalesOrder(Long id) {
                log.info("Getting sales order: {}", id);

                SalesOrder order = salesOrderRepository.findByIdAndDeletedFalse(id)
                                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.SALES_ORDER_NOT_FOUND));

                return SalesOrderResponse.from(order);
        }

        /**
         * 수주 목록 조회 (페이징)
         *
         * 삭제되지 않은 수주 목록을 페이징하여 조회합니다.
         * 최신 생성순으로 정렬됩니다.
         *
         * @param page 페이지 번호 (0부터 시작)
         * @param size 페이지당 항목 수
         * @return 페이징된 수주 목록
         */
        public PageResponse<SalesOrderResponse> getSalesOrders(int page, int size) {
                log.info("Getting sales orders - page: {}, size: {}", page, size);

                Pageable pageable = PageRequest.of(page, size);
                Page<SalesOrder> orderPage = salesOrderRepository.findAllActive(pageable);

                List<SalesOrderResponse> responses = orderPage.getContent().stream()
                                .map(SalesOrderResponse::from)
                                .collect(Collectors.toList());

                return PageResponse.of(responses, page, size, orderPage.getTotalElements());
        }

        /**
         * 수주 수정
         *
         * 기존 수주의 정보를 수정합니다.
         * 확정된 수주는 수정할 수 없습니다 (비즈니스 규칙).
         *
         * @param id      수주 ID
         * @param request 수정할 수주 정보
         * @return 수정된 수주 정보
         * @throws ResourceNotFoundException 수주를 찾을 수 없는 경우
         * @throws BusinessException         확정된 수주를 수정하려는 경우
         */
        @Transactional
        public SalesOrderResponse updateSalesOrder(Long id, SalesOrderRequest request) {
                log.info("Updating sales order: {}", id);

                SalesOrder order = salesOrderRepository.findByIdAndDeletedFalse(id)
                                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.SALES_ORDER_NOT_FOUND));

                // 비즈니스 규칙: 확정된 수주는 수정 불가
                if (order.getStatus() == SalesOrder.OrderStatus.CONFIRMED) {
                        throw new BusinessException(ErrorCode.SALES_ORDER_ALREADY_CONFIRMED,
                                        "확정된 수주는 수정할 수 없습니다");
                }

                // 수주 헤더 정보 수정
                order.setOrderDate(request.getOrderDate());
                order.setCustomerCode(request.getCustomerCode());
                order.setCustomerName(request.getCustomerName());
                order.setDeliveryAddress(request.getDeliveryAddress());
                order.setRemarks(request.getRemarks());

                // 수주 라인 재설정 (기존 라인 모두 삭제 후 새로 추가)
                order.getLines().clear();
                request.getLines().forEach(lineReq -> {
                        SalesOrderLine line = new SalesOrderLine(
                                        lineReq.getLineNumber(),
                                        lineReq.getItemCode(),
                                        lineReq.getItemName(),
                                        lineReq.getQuantity(),
                                        lineReq.getUnitPrice(),
                                        lineReq.getRemarks());
                        order.addLine(line);
                });

                SalesOrder updatedOrder = salesOrderRepository.save(order);
                log.info("Sales order updated: {}", updatedOrder.getOrderNumber());

                return SalesOrderResponse.from(updatedOrder);
        }

        /**
         * 수주 확정
         *
         * 대기(PENDING) 상태의 수주를 확정(CONFIRMED) 상태로 변경합니다.
         * 확정된 수주는 더 이상 수정할 수 없습니다.
         *
         * @param id 수주 ID
         * @return 확정된 수주 정보
         * @throws ResourceNotFoundException 수주를 찾을 수 없는 경우
         * @throws IllegalStateException     이미 확정되었거나 취소된 수주인 경우
         */
        @Transactional
        public SalesOrderResponse confirmSalesOrder(Long id) {
                log.info("Confirming sales order: {}", id);

                SalesOrder order = salesOrderRepository.findByIdAndDeletedFalse(id)
                                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.SALES_ORDER_NOT_FOUND));

                order.confirm();
                SalesOrder confirmedOrder = salesOrderRepository.save(order);

                log.info("Sales order confirmed: {}", confirmedOrder.getOrderNumber());

                return SalesOrderResponse.from(confirmedOrder);
        }

        /**
         * 수주 삭제 (Soft Delete)
         *
         * 수주를 논리적으로 삭제합니다 (실제 데이터는 유지).
         * 출하완료된 수주는 삭제할 수 없습니다 (비즈니스 규칙).
         *
         * @param id 수주 ID
         * @throws ResourceNotFoundException 수주를 찾을 수 없는 경우
         * @throws BusinessException         출하완료된 수주를 삭제하려는 경우
         */
        @Transactional
        public void deleteSalesOrder(Long id) {
                log.info("Deleting sales order: {}", id);

                SalesOrder order = salesOrderRepository.findByIdAndDeletedFalse(id)
                                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.SALES_ORDER_NOT_FOUND));

                // 비즈니스 규칙: 출하완료된 수주는 삭제 불가
                if (order.getStatus() == SalesOrder.OrderStatus.SHIPPED) {
                        throw new BusinessException(ErrorCode.INVALID_SALES_ORDER,
                                        "출하완료된 수주는 삭제할 수 없습니다");
                }

                order.softDelete();
                salesOrderRepository.save(order);

                log.info("Sales order deleted: {}", order.getOrderNumber());
        }

        /**
         * 수주번호 자동 생성
         *
         * 형식: SO-{연도}-{순번}
         * 예시: SO-2025-1001, SO-2025-1002
         *
         * 데이터베이스에서 해당 연도의 최대 순번을 조회하여
         * 다음 순번을 생성합니다. 이를 통해 서버 재시작 시에도
         * 중복되지 않는 고유한 수주번호를 보장합니다.
         *
         * @return 생성된 수주번호
         */
        private String generateOrderNumber() {
                int currentYear = LocalDateTime.now().getYear();
                String yearPattern = String.format("SO-%04d-%%", currentYear);

                // DB에서 해당 연도의 최대 순번 조회
                Integer maxSequence = salesOrderRepository.findMaxSequenceByYear(yearPattern);
                if (maxSequence == null) {
                        maxSequence = 0;
                }

                // 다음 순번 생성 (최소 1001부터 시작)
                int nextSequence = Math.max(maxSequence + 1, 1001);

                return String.format("SO-%04d-%04d", currentYear, nextSequence);
        }
}
