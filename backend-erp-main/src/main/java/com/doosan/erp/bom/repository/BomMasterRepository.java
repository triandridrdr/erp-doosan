package com.doosan.erp.bom.repository;

import com.doosan.erp.bom.entity.BomMaster;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface BomMasterRepository extends JpaRepository<BomMaster, Long> {

    List<BomMaster> findByStyleNoContainingIgnoreCaseAndArticleContainingIgnoreCase(String styleNo, String article);
}
