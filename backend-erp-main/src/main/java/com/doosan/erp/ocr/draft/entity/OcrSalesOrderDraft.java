package com.doosan.erp.ocr.draft.entity;

import com.doosan.erp.common.entity.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
@Entity
@Table(name = "ocr_sales_order_drafts")
@Getter
@Setter
@NoArgsConstructor
public class OcrSalesOrderDraft extends BaseEntity {

    public enum DraftStatus {
        DRAFT,
        APPROVED
    }

    @Column(name = "source_filename", length = 255)
    private String sourceFilename;

    @Column(name = "so_number", length = 50)
    private String soNumber;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private DraftStatus status = DraftStatus.DRAFT;

    @Lob
    @Column(name = "draft_json", columnDefinition = "LONGTEXT", nullable = false)
    private String draftJson;
}
