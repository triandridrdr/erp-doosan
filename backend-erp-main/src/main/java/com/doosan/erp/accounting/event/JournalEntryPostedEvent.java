package com.doosan.erp.accounting.event;

import com.doosan.erp.common.event.DomainEvent;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 회계전표 전기 이벤트
 * 전표가 전기될 때 발행되는 도메인 이벤트
 */
@Getter
@AllArgsConstructor
public class JournalEntryPostedEvent implements DomainEvent {

    private final Long entryId;
    private final String entryNumber;
    private final BigDecimal totalAmount;
    private final LocalDateTime occurredAt;
}
