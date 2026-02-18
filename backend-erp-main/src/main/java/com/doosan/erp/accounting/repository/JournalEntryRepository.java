package com.doosan.erp.accounting.repository;

import com.doosan.erp.accounting.entity.JournalEntry;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * 회계전표 Repository
 */
@Repository
public interface JournalEntryRepository extends JpaRepository<JournalEntry, Long> {

    /**
     * ID로 조회 (삭제되지 않은 항목만)
     */
    Optional<JournalEntry> findByIdAndDeletedFalse(Long id);

    /**
     * 전표번호로 조회
     */
    Optional<JournalEntry> findByEntryNumberAndDeletedFalse(String entryNumber);

    /**
     * 삭제되지 않은 전표 목록 조회 (페이징)
     */
    @Query("SELECT je FROM JournalEntry je WHERE je.deleted = false ORDER BY je.createdAt DESC")
    Page<JournalEntry> findAllActive(Pageable pageable);

    /**
     * 삭제되지 않은 전표 개수
     */
    long countByDeletedFalse();
}
