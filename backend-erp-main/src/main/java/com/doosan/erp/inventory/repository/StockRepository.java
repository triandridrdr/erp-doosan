package com.doosan.erp.inventory.repository;

import com.doosan.erp.inventory.entity.Stock;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * 재고 Repository
 */
@Repository
public interface StockRepository extends JpaRepository<Stock, Long> {

    /**
     * ID로 조회 (삭제되지 않은 항목만)
     */
    Optional<Stock> findByIdAndDeletedFalse(Long id);

    /**
     * 품목코드와 창고코드로 조회
     */
    Optional<Stock> findByItemCodeAndWarehouseCodeAndDeletedFalse(String itemCode, String warehouseCode);

    /**
     * 품목코드로 조회
     */
    List<Stock> findByItemCodeAndDeletedFalse(String itemCode);

    /**
     * 창고코드로 조회
     */
    List<Stock> findByWarehouseCodeAndDeletedFalse(String warehouseCode);

    /**
     * 전체 조회 (삭제되지 않은 항목만)
     */
    List<Stock> findByDeletedFalse();
}
