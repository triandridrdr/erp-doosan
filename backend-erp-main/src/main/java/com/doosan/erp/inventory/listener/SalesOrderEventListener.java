package com.doosan.erp.inventory.listener;

import com.doosan.erp.inventory.service.StockService;
import com.doosan.erp.sales.event.SalesOrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * 수주 이벤트 리스너
 *
 * Sales 도메인에서 발행하는 이벤트를 수신하여 재고 처리를 수행하는 리스너입니다.
 * Modular Monolith 아키텍처의 모듈 간 통신 예시로, 이벤트 기반 비동기 처리를 구현합니다.
 *
 * 주요 기능:
 * - 수주 생성 이벤트 수신: SalesOrderCreatedEvent를 비동기로 처리
 * - 재고 예약: 수주 생성 시 해당 품목의 재고를 자동으로 예약
 *
 * @Async 어노테이션으로 비동기 처리되며, 실패 시 로깅만 수행합니다.
 * 실제 운영 환경에서는 보상 트랜잭션 처리 또는 알림 발송이 필요할 수 있습니다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SalesOrderEventListener {

    // 재고 비즈니스 로직을 처리하는 서비스
    private final StockService stockService;

    /**
     * 수주 생성 이벤트 처리
     *
     * 수주가 생성되면 해당 품목의 재고를 예약 처리합니다.
     * 각 수주 라인별로 품목코드와 수량을 확인하여 재고를 예약합니다.
     *
     * 처리 순서:
     * 1. 수주 생성 이벤트 수신
     * 2. 각 라인별로 재고 예약 처리
     * 3. 실패 시 에러 로깅 (보상 트랜잭션은 별도 구현 필요)
     *
     * @param event 수주 생성 이벤트 (주문번호, 라인 정보 포함)
     */
    @EventListener
    @Async("taskExecutor")
    public void handleSalesOrderCreated(SalesOrderCreatedEvent event) {
        log.info("수주 생성 이벤트 수신 - 주문번호: {}", event.getOrderNumber());

        try {
            // 각 라인별로 재고 예약
            event.getLines().forEach(line -> {
                log.info("재고 예약 처리 - 품목: {}, 수량: {}",
                        line.getItemCode(), line.getQuantity());

                // 기본 창고에서 재고 예약
                String defaultWarehouse = "WH-001";
                stockService.reserveStock(
                        line.getItemCode(),
                        defaultWarehouse,
                        line.getQuantity()
                );
            });

            log.info("재고 예약 완료 - 주문번호: {}", event.getOrderNumber());

        } catch (Exception e) {
            log.error("재고 예약 실패 - 주문번호: {}, 에러: {}",
                    event.getOrderNumber(), e.getMessage(), e);
            // 실제 운영 환경에서는 보상 트랜잭션 처리 또는 알림 발송
        }
    }
}
