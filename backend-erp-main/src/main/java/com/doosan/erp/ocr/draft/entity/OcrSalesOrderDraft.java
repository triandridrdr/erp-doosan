package com.doosan.erp.ocr.draft.entity;

import com.doosan.erp.common.entity.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
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

    @Column(name = "source_filename", length = 255)
    private String sourceFilename;

    @Column(name = "so_number", length = 50)
    private String soNumber;

    @Lob
    @Column(name = "draft_json", columnDefinition = "LONGTEXT", nullable = false)
    private String draftJson;
}
