package com.doosan.erp.bom.entity;

import com.doosan.erp.common.entity.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "bom_masters")
@Getter
@Setter
@NoArgsConstructor
public class BomMaster extends BaseEntity {

    public enum BomStatus {
        DRAFT,
        ACTIVE,
        INACTIVE
    }

    @Column(name = "style_no", length = 100, nullable = false)
    private String styleNo;

    @Column(name = "article", length = 100, nullable = false)
    private String article;

    @Column(name = "revision", nullable = false)
    private Integer revision = 1;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 20, nullable = false)
    private BomStatus status = BomStatus.DRAFT;
}
