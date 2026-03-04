package com.doosan.erp.bom.repository;

import com.doosan.erp.bom.entity.BomMasterLine;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface BomMasterLineRepository extends JpaRepository<BomMasterLine, Long> {

    List<BomMasterLine> findByBomMasterIdOrderByLineNoAsc(Long bomMasterId);
}
