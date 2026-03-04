package com.doosan.erp.bom.entity;

import com.doosan.erp.common.entity.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "bom_master_lines")
@Getter
@Setter
@NoArgsConstructor
public class BomMasterLine extends BaseEntity {

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "bom_master_id", nullable = false)
    private BomMaster bomMaster;

    @Column(name = "line_no", nullable = false)
    private Integer lineNo;

    @Column(name = "component", length = 200)
    private String component;

    @Column(name = "category", length = 100)
    private String category;

    @Column(name = "composition", length = 2000)
    private String composition;

    @Column(name = "uom", length = 30)
    private String uom;

    @Column(name = "consumption_per_unit", length = 50)
    private String consumptionPerUnit;

    @Column(name = "waste_percent", length = 50)
    private String wastePercent;
}
