package com.doosan.erp.accounting.service;

import com.doosan.erp.accounting.dto.JournalEntryRequest;
import com.doosan.erp.accounting.dto.JournalEntryResponse;
import com.doosan.erp.accounting.entity.JournalEntry;
import com.doosan.erp.accounting.entity.JournalEntryLine;
import com.doosan.erp.accounting.event.JournalEntryPostedEvent;
import com.doosan.erp.accounting.repository.JournalEntryRepository;
import com.doosan.erp.common.constant.ErrorCode;
import com.doosan.erp.common.dto.PageResponse;
import com.doosan.erp.common.exception.BusinessException;
import com.doosan.erp.common.exception.ResourceNotFoundException;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 회계전표 서비스
 *
 * 회계전표(Journal Entry) 관련 비즈니스 로직을 처리하는 서비스 클래스입니다.
 * 트랜잭션 관리, 비즈니스 규칙 검증, 도메인 이벤트 발행 등을 담당합니다.
 *
 * 주요 기능:
 * - 전표 생성: 전표번호 자동 생성, 라인 추가, 차대변 균형 검증
 * - 전표 조회: 단건/목록 조회 (페이징 지원)
 * - 전표 전기: 작성중 전표를 전기완료로 변경, 이벤트 발행
 * - 전표 삭제: Soft Delete (전기된 전표 삭제 불가)
 *
 * Transactional(readOnly = true): 기본적으로 읽기 전용 트랜잭션 적용
 * 쓰기 작업 메서드에는 별도로 Transactional 어노테이션 적용
 */
@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class JournalEntryService {

    // 전표 데이터베이스 접근을 위한 Repository
    private final JournalEntryRepository journalEntryRepository;

    // 도메인 이벤트 발행을 위한 Spring의 이벤트 퍼블리셔
    private final ApplicationEventPublisher eventPublisher;

    // 전표번호 생성을 위한 순차 번호 (동시성 안전한 AtomicInteger 사용)
    private final AtomicInteger entryNumberSequence = new AtomicInteger(1000);

    /**
     * 회계전표 생성
     *
     * 새로운 회계전표를 생성합니다.
     * 처리 순서:
     * 1. 전표 엔티티 생성 및 기본 정보 설정
     * 2. 전표 라인(분개 항목) 추가
     * 3. 차대변 균형 검증 (차변 합계 = 대변 합계)
     * 4. 데이터베이스 저장
     *
     * @param request 전표 생성 요청 정보
     * @return 생성된 전표 정보
     * @throws BusinessException 차변과 대변이 일치하지 않는 경우
     */
    @Transactional
    public JournalEntryResponse createJournalEntry(JournalEntryRequest request) {
        log.info("Creating journal entry for date: {}", request.getEntryDate());

        // 엔티티 생성
        JournalEntry entry = new JournalEntry();
        entry.setEntryNumber(generateEntryNumber());
        entry.setEntryDate(request.getEntryDate());
        entry.setDescription(request.getDescription());
        entry.setStatus(JournalEntry.EntryStatus.DRAFT);

        // 라인 추가
        request.getLines().forEach(lineReq -> {
            JournalEntryLine line = new JournalEntryLine(
                    lineReq.getLineNumber(),
                    lineReq.getAccountCode(),
                    lineReq.getAccountName(),
                    lineReq.getDebit(),
                    lineReq.getCredit(),
                    lineReq.getDescription()
            );
            entry.addLine(line);
        });

        // 차대평균 검증
        if (!entry.isBalanced()) {
            throw new BusinessException(ErrorCode.INVALID_JOURNAL_ENTRY);
        }

        // 저장
        JournalEntry savedEntry = journalEntryRepository.save(entry);
        log.info("Journal entry created: {}", savedEntry.getEntryNumber());

        return JournalEntryResponse.from(savedEntry);
    }

    /**
     * 회계전표 조회
     *
     * ID로 전표를 조회합니다. 삭제된 전표는 조회되지 않습니다.
     *
     * @param id 전표 ID
     * @return 전표 상세 정보
     * @throws ResourceNotFoundException 전표를 찾을 수 없는 경우
     */
    public JournalEntryResponse getJournalEntry(Long id) {
        log.info("Getting journal entry: {}", id);

        JournalEntry entry = journalEntryRepository.findByIdAndDeletedFalse(id)
                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.JOURNAL_ENTRY_NOT_FOUND));

        return JournalEntryResponse.from(entry);
    }

    /**
     * 회계전표 목록 조회 (페이징)
     *
     * 삭제되지 않은 전표 목록을 페이징하여 조회합니다.
     * 최신 생성순으로 정렬됩니다.
     *
     * @param page 페이지 번호 (0부터 시작)
     * @param size 페이지당 항목 수
     * @return 페이징된 전표 목록
     */
    public PageResponse<JournalEntryResponse> getJournalEntries(int page, int size) {
        log.info("Getting journal entries - page: {}, size: {}", page, size);

        Pageable pageable = PageRequest.of(page, size);
        Page<JournalEntry> entryPage = journalEntryRepository.findAllActive(pageable);

        List<JournalEntryResponse> responses = entryPage.getContent().stream()
                .map(JournalEntryResponse::from)
                .collect(Collectors.toList());

        return PageResponse.of(responses, page, size, entryPage.getTotalElements());
    }

    /**
     * 회계전표 전기
     *
     * 작성중(DRAFT) 상태의 전표를 전기완료(POSTED) 상태로 변경합니다.
     * 전기 시 차대변 균형을 재검증하고, 전기 이벤트를 발행합니다.
     *
     * @param id 전표 ID
     * @return 전기된 전표 정보
     * @throws ResourceNotFoundException 전표를 찾을 수 없는 경우
     * @throws IllegalStateException 이미 전기되었거나 차대변 불일치인 경우
     */
    @Transactional
    public JournalEntryResponse postJournalEntry(Long id) {
        log.info("Posting journal entry: {}", id);

        JournalEntry entry = journalEntryRepository.findByIdAndDeletedFalse(id)
                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.JOURNAL_ENTRY_NOT_FOUND));

        // 전기 처리
        entry.post();
        JournalEntry postedEntry = journalEntryRepository.save(entry);

        // 이벤트 발행
        JournalEntryPostedEvent event = new JournalEntryPostedEvent(
                postedEntry.getId(),
                postedEntry.getEntryNumber(),
                postedEntry.getTotalDebit(),
                LocalDateTime.now()
        );
        eventPublisher.publishEvent(event);

        log.info("Journal entry posted: {}", postedEntry.getEntryNumber());

        return JournalEntryResponse.from(postedEntry);
    }

    /**
     * 회계전표 삭제 (Soft Delete)
     *
     * 전표를 논리적으로 삭제합니다 (실제 데이터는 유지).
     * 전기된 전표는 삭제할 수 없습니다 (비즈니스 규칙).
     *
     * @param id 전표 ID
     * @throws ResourceNotFoundException 전표를 찾을 수 없는 경우
     * @throws BusinessException 전기된 전표를 삭제하려는 경우
     */
    @Transactional
    public void deleteJournalEntry(Long id) {
        log.info("Deleting journal entry: {}", id);

        JournalEntry entry = journalEntryRepository.findByIdAndDeletedFalse(id)
                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.JOURNAL_ENTRY_NOT_FOUND));

        if (entry.getStatus() == JournalEntry.EntryStatus.POSTED) {
            throw new BusinessException(ErrorCode.JOURNAL_ENTRY_ALREADY_POSTED,
                    "전기된 전표는 삭제할 수 없습니다");
        }

        entry.softDelete();
        journalEntryRepository.save(entry);

        log.info("Journal entry deleted: {}", entry.getEntryNumber());
    }

    /**
     * 전표번호 자동 생성
     *
     * 형식: JE-{연도}-{순번}
     * 예시: JE-2024-1001, JE-2024-1002
     *
     * AtomicInteger를 사용하여 동시성 환경에서도 안전하게 순번 생성
     *
     * @return 생성된 전표번호
     */
    private String generateEntryNumber() {
        return String.format("JE-%04d-%04d",
                LocalDateTime.now().getYear(),
                entryNumberSequence.incrementAndGet());
    }
}
