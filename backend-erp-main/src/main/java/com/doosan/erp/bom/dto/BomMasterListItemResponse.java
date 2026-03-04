package com.doosan.erp.bom.dto;

import com.doosan.erp.bom.entity.BomMaster;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
@AllArgsConstructor
public class BomMasterListItemResponse {

    private Long id;
    private String styleNo;
    private String article;
    private Integer revision;
    private BomMaster.BomStatus status;
    private LocalDateTime createdAt;

    public static BomMasterListItemResponse from(BomMaster e) {
        return new BomMasterListItemResponse(
                e.getId(),
                e.getStyleNo(),
                e.getArticle(),
                e.getRevision(),
                e.getStatus(),
                e.getCreatedAt()
        );
    }
}
