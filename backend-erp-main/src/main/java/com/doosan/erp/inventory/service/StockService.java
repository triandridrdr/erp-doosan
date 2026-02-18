package com.doosan.erp.inventory.service;

import com.doosan.erp.common.constant.ErrorCode;
import com.doosan.erp.common.exception.BusinessException;
import com.doosan.erp.common.exception.ResourceNotFoundException;
import com.doosan.erp.inventory.dto.StockRequest;
import com.doosan.erp.inventory.dto.StockResponse;
import com.doosan.erp.inventory.entity.Stock;
import com.doosan.erp.inventory.repository.StockRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 재고 서비스
 *
 * 재고(Stock) 관련 비즈니스 로직을 처리하는 서비스 클래스입니다.
 * 트랜잭션 관리, 비즈니스 규칙 검증 등을 담당합니다.
 *
 * 주요 기능:
 * - 재고 생성: 품목별, 창고별 재고 생성
 * - 재고 조회: 단건/목록 조회 (품목별, 창고별 필터링 지원)
 * - 재고 예약: 수주 생성 시 재고 예약
 * - 재고 예약 해제: 수주 취소 시 예약 해제
 * - 재고 차감: 실제 출고 시 재고 차감
 * - 재고 증가: 입고 시 재고 증가
 *
 * 다른 모듈(sales 등)에서도 사용 가능하도록 설계되었습니다.
 *
 * Transactional(readOnly = true): 기본적으로 읽기 전용 트랜잭션 적용
 * 쓰기 작업 메서드에는 별도로 Transactional 어노테이션 적용
 */
@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class StockService {

    // 재고 데이터베이스 접근을 위한 Repository
    private final StockRepository stockRepository;

    /**
     * 재고 생성
     *
     * 새로운 재고를 생성합니다.
     * 동일한 품목코드와 창고코드 조합이 이미 존재하면 예외를 발생시킵니다.
     *
     * @param request 재고 생성 요청 정보
     * @return 생성된 재고 정보
     * @throws BusinessException 동일한 품목+창고 조합이 이미 존재하는 경우
     */
    @Transactional
    public StockResponse createStock(StockRequest request) {
        log.info("Creating stock - item: {}, warehouse: {}",
                request.getItemCode(), request.getWarehouseCode());

        // 동일한 품목코드 + 창고코드 조합이 이미 있는지 확인
        stockRepository.findByItemCodeAndWarehouseCodeAndDeletedFalse(
                request.getItemCode(), request.getWarehouseCode())
                .ifPresent(stock -> {
                    throw new BusinessException(ErrorCode.DUPLICATE_RESOURCE,
                            String.format("이미 존재하는 재고입니다 (품목: %s, 창고: %s)",
                                    request.getItemCode(), request.getWarehouseCode()));
                });

        Stock stock = new Stock(
                request.getItemCode(),
                request.getItemName(),
                request.getWarehouseCode(),
                request.getWarehouseName(),
                request.getQuantity(),
                request.getUnit(),
                request.getUnitPrice());

        Stock saved = stockRepository.save(stock);
        log.info("Stock created - id: {}, item: {}", saved.getId(), saved.getItemCode());

        return StockResponse.from(saved);
    }

    /**
     * 재고 조회
     *
     * ID로 재고를 조회합니다. 삭제된 재고는 조회되지 않습니다.
     *
     * @param id 재고 ID
     * @return 재고 상세 정보
     * @throws ResourceNotFoundException 재고를 찾을 수 없는 경우
     */
    public StockResponse getStock(Long id) {
        log.info("Getting stock: {}", id);

        Stock stock = stockRepository.findByIdAndDeletedFalse(id)
                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.STOCK_NOT_FOUND));

        return StockResponse.from(stock);
    }

    /**
     * 품목별 재고 조회
     *
     * 특정 품목의 모든 창고별 재고 현황을 조회합니다.
     *
     * @param itemCode 품목 코드
     * @return 해당 품목의 재고 목록
     */
    public List<StockResponse> getStocksByItemCode(String itemCode) {
        log.info("Getting stocks by item code: {}", itemCode);

        List<Stock> stocks = stockRepository.findByItemCodeAndDeletedFalse(itemCode);
        return stocks.stream()
                .map(StockResponse::from)
                .collect(Collectors.toList());
    }

    /**
     * 창고별 재고 조회
     *
     * 특정 창고의 모든 품목별 재고 현황을 조회합니다.
     *
     * @param warehouseCode 창고 코드
     * @return 해당 창고의 재고 목록
     */
    public List<StockResponse> getStocksByWarehouseCode(String warehouseCode) {
        log.info("Getting stocks by warehouse code: {}", warehouseCode);

        List<Stock> stocks = stockRepository.findByWarehouseCodeAndDeletedFalse(warehouseCode);
        return stocks.stream()
                .map(StockResponse::from)
                .collect(Collectors.toList());
    }

    /**
     * 전체 재고 조회
     *
     * 삭제되지 않은 모든 재고를 조회합니다.
     *
     * @return 전체 재고 목록
     */
    public List<StockResponse> getAllStocks() {
        log.info("Getting all stocks");

        List<Stock> stocks = stockRepository.findByDeletedFalse();
        return stocks.stream()
                .map(StockResponse::from)
                .collect(Collectors.toList());
    }

    /**
     * 재고 예약 (수주 생성 시 호출)
     *
     * 수주 생성 시 가용 재고를 예약합니다.
     * 가용 재고가 부족하면 예외를 발생시킵니다.
     *
     * @param itemCode      품목 코드
     * @param warehouseCode 창고 코드
     * @param quantity      예약할 수량
     * @throws ResourceNotFoundException 재고를 찾을 수 없는 경우
     * @throws BusinessException         가용 재고가 부족한 경우
     */
    @Transactional
    public void reserveStock(String itemCode, String warehouseCode, BigDecimal quantity) {
        log.info("Reserving stock - item: {}, warehouse: {}, quantity: {}",
                itemCode, warehouseCode, quantity);

        Stock stock = stockRepository.findByItemCodeAndWarehouseCodeAndDeletedFalse(itemCode, warehouseCode)
                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.STOCK_NOT_FOUND,
                        String.format("재고를 찾을 수 없습니다 (품목: %s, 창고: %s)", itemCode, warehouseCode)));

        try {
            stock.reserve(quantity);
            stockRepository.save(stock);
            log.info("Stock reserved - item: {}, quantity: {}", itemCode, quantity);
        } catch (IllegalStateException e) {
            throw new BusinessException(ErrorCode.INSUFFICIENT_STOCK, e.getMessage());
        }
    }

    /**
     * 재고 예약 해제
     *
     * 수주 취소 등으로 인해 예약된 재고를 해제합니다.
     * 예약 수량이 부족하면 예외를 발생시킵니다.
     *
     * @param itemCode      품목 코드
     * @param warehouseCode 창고 코드
     * @param quantity      해제할 수량
     * @throws ResourceNotFoundException 재고를 찾을 수 없는 경우
     * @throws BusinessException         예약 수량이 부족한 경우
     */
    @Transactional
    public void releaseReservation(String itemCode, String warehouseCode, BigDecimal quantity) {
        log.info("Releasing reservation - item: {}, warehouse: {}, quantity: {}",
                itemCode, warehouseCode, quantity);

        Stock stock = stockRepository.findByItemCodeAndWarehouseCodeAndDeletedFalse(itemCode, warehouseCode)
                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.STOCK_NOT_FOUND));

        try {
            stock.releaseReservation(quantity);
            stockRepository.save(stock);
            log.info("Reservation released - item: {}, quantity: {}", itemCode, quantity);
        } catch (IllegalStateException e) {
            throw new BusinessException(ErrorCode.INVALID_STOCK_QUANTITY, e.getMessage());
        }
    }

    /**
     * 재고 차감 (실제 출고)
     *
     * 실제 출고 시 총 재고와 예약 재고를 차감합니다.
     * 예약된 재고만 출고할 수 있습니다.
     *
     * @param itemCode      품목 코드
     * @param warehouseCode 창고 코드
     * @param quantity      차감할 수량
     * @throws ResourceNotFoundException 재고를 찾을 수 없는 경우
     * @throws BusinessException         예약 재고가 부족한 경우
     */
    @Transactional
    public void deductStock(String itemCode, String warehouseCode, BigDecimal quantity) {
        log.info("Deducting stock - item: {}, warehouse: {}, quantity: {}",
                itemCode, warehouseCode, quantity);

        Stock stock = stockRepository.findByItemCodeAndWarehouseCodeAndDeletedFalse(itemCode, warehouseCode)
                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.STOCK_NOT_FOUND));

        try {
            stock.deduct(quantity);
            stockRepository.save(stock);
            log.info("Stock deducted - item: {}, quantity: {}", itemCode, quantity);
        } catch (IllegalStateException e) {
            throw new BusinessException(ErrorCode.INSUFFICIENT_STOCK, e.getMessage());
        }
    }

    /**
     * 재고 증가 (입고)
     *
     * 입고 시 총 재고와 가용 재고를 증가시킵니다.
     *
     * @param itemCode      품목 코드
     * @param warehouseCode 창고 코드
     * @param quantity      증가할 수량
     * @throws ResourceNotFoundException 재고를 찾을 수 없는 경우
     */
    @Transactional
    public void increaseStock(String itemCode, String warehouseCode, BigDecimal quantity) {
        log.info("Increasing stock - item: {}, warehouse: {}, quantity: {}",
                itemCode, warehouseCode, quantity);

        Stock stock = stockRepository.findByItemCodeAndWarehouseCodeAndDeletedFalse(itemCode, warehouseCode)
                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.STOCK_NOT_FOUND));

        stock.increase(quantity);
        stockRepository.save(stock);
        log.info("Stock increased - item: {}, quantity: {}", itemCode, quantity);
    }
}
