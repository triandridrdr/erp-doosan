package com.doosan.erp.bom.service;

import com.doosan.erp.bom.dto.BomMasterCreateRequest;
import com.doosan.erp.bom.dto.BomMasterLineRequest;
import com.doosan.erp.bom.dto.BomMasterLineResponse;
import com.doosan.erp.bom.dto.BomMasterResponse;
import com.doosan.erp.bom.entity.BomMaster;
import com.doosan.erp.bom.entity.BomMasterLine;
import com.doosan.erp.bom.repository.BomMasterLineRepository;
import com.doosan.erp.bom.repository.BomMasterRepository;
import com.doosan.erp.common.constant.ErrorCode;
import com.doosan.erp.common.exception.ResourceNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class BomMasterService {

    private final BomMasterRepository bomMasterRepository;
    private final BomMasterLineRepository bomMasterLineRepository;

    public List<BomMaster> search(String styleNo, String article) {
        String s = styleNo == null ? "" : styleNo;
        String a = article == null ? "" : article;
        return bomMasterRepository.findByStyleNoContainingIgnoreCaseAndArticleContainingIgnoreCase(s, a)
                .stream()
                .sorted(Comparator.comparing(BomMaster::getCreatedAt, Comparator.nullsLast(Comparator.naturalOrder())).reversed())
                .collect(Collectors.toList());
    }

    public BomMasterResponse get(Long id) {
        BomMaster m = bomMasterRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.RESOURCE_NOT_FOUND, "BoM master not found"));
        List<BomMasterLineResponse> lines = bomMasterLineRepository.findByBomMasterIdOrderByLineNoAsc(id)
                .stream()
                .map(BomMasterLineResponse::from)
                .collect(Collectors.toList());
        return new BomMasterResponse(
                m.getId(),
                m.getStyleNo(),
                m.getArticle(),
                m.getRevision(),
                m.getStatus(),
                m.getCreatedAt(),
                lines
        );
    }

    @Transactional
    public BomMasterResponse create(BomMasterCreateRequest request) {
        BomMaster m = new BomMaster();
        m.setStyleNo(request.getStyleNo());
        m.setArticle(request.getArticle());
        m.setRevision(1);
        m.setStatus(BomMaster.BomStatus.ACTIVE);
        BomMaster saved = bomMasterRepository.save(m);

        for (BomMasterLineRequest lr : request.getLines()) {
            BomMasterLine line = new BomMasterLine();
            line.setBomMaster(saved);
            line.setLineNo(lr.getLineNo());
            line.setComponent(lr.getComponent());
            line.setCategory(lr.getCategory());
            line.setComposition(lr.getComposition());
            line.setUom(lr.getUom());
            line.setConsumptionPerUnit(lr.getConsumptionPerUnit());
            line.setWastePercent(lr.getWastePercent());
            bomMasterLineRepository.save(line);
        }

        return get(saved.getId());
    }
}
