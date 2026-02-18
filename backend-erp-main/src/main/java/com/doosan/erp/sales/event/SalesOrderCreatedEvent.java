package com.doosan.erp.sales.event;

import com.doosan.erp.common.event.DomainEvent;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * 수주 생성 이벤트
 *
 * 수주가 생성될 때 발행되는 도메인 이벤트입니다.
 * 이 이벤트는 다른 모듈(예: 재고 모듈)에서 수신하여 후속 처리를 수행합니다.
 *
 * 사용 예시:
 * - 재고 모듈: 수주 생성 시 재고 예약 처리
 * - 알림 모듈: 담당자에게 수주 생성 알림 발송
 *
 * DomainEvent 인터페이스를 구현하여 이벤트 발생 시각을 제공합니다.
 */
@Getter
@AllArgsConstructor
public class SalesOrderCreatedEvent implements DomainEvent {

    private final Long orderId;           // 수주 ID
    private final String orderNumber;     // 수주번호
    private final List<OrderLineInfo> lines;  // 주문 품목 목록
    private final LocalDateTime occurredAt;   // 이벤트 발생 시각

    /**
     * 주문 품목 정보
     *
     * 이벤트에 포함되는 품목별 간략 정보입니다.
     * 재고 모듈에서 재고 예약 시 사용됩니다.
     */
    @Getter
    @AllArgsConstructor
    public static class OrderLineInfo {
        private final String itemCode;      // 품목코드
        private final BigDecimal quantity;  // 주문 수량
    }
}
