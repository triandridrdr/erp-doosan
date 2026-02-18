package com.doosan.erp.common.event;

import java.time.LocalDateTime;

/**
 * 도메인 이벤트 인터페이스
 * 모든 도메인 이벤트가 구현해야 하는 마커 인터페이스
 */
public interface DomainEvent {

    /**
     * 이벤트 발생 시간 반환
     */
    LocalDateTime getOccurredAt();
}
