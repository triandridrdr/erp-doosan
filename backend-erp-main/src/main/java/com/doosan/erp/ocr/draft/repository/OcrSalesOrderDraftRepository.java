package com.doosan.erp.ocr.draft.repository;

import com.doosan.erp.ocr.draft.entity.OcrSalesOrderDraft;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OcrSalesOrderDraftRepository extends JpaRepository<OcrSalesOrderDraft, Long> {
}
