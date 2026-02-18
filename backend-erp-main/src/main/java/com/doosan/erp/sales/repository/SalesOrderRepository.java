package com.doosan.erp.sales.repository;

import com.doosan.erp.sales.entity.SalesOrder;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * 수주 Repository 인터페이스
 *
 * 수주(SalesOrder) 엔티티에 대한 데이터베이스 접근을 담당합니다.
 * JpaRepository를 상속하여 기본 CRUD와 페이징 기능을 제공받습니다.
 *
 * Soft Delete 패턴을 적용하여 모든 조회 메서드에서 deleted=false 조건을 사용합니다.
 * 이를 통해 삭제된 데이터는 조회되지 않지만 데이터베이스에는 보존됩니다.
 */
@Repository
public interface SalesOrderRepository extends JpaRepository<SalesOrder, Long> {

    /**
     * 수주번호로 조회
     *
     * 수주번호는 고유값이므로 단건 조회에 사용됩니다.
     * 외부 시스템과 연동 시 주로 이 메서드를 사용합니다.
     *
     * @param orderNumber 수주번호 (예: SO-2024-1001)
     * @return Optional로 감싼 수주 엔티티
     */
    Optional<SalesOrder> findByOrderNumberAndDeletedFalse(String orderNumber);

    /**
     * ID로 삭제되지 않은 수주 조회
     *
     * 내부 시스템에서 PK인 ID로 수주를 조회할 때 사용합니다.
     *
     * @param id 수주 ID (PK)
     * @return Optional로 감싼 수주 엔티티
     */
    Optional<SalesOrder> findByIdAndDeletedFalse(Long id);

    /**
     * 활성 수주 목록 조회 (페이징)
     *
     * 삭제되지 않은 모든 수주를 최신순으로 페이징하여 조회합니다.
     * JPQL을 사용하여 정렬 조건을 명시적으로 지정합니다.
     *
     * @param pageable 페이징 정보
     * @return 페이징된 수주 목록
     */
    @Query("SELECT so FROM SalesOrder so WHERE so.deleted = false ORDER BY so.createdAt DESC")
    Page<SalesOrder> findAllActive(Pageable pageable);

    /**
     * 활성 수주 개수 조회
     *
     * 삭제되지 않은 수주의 총 개수를 반환합니다.
     * 통계나 대시보드에서 활용됩니다.
     */
    long countByDeletedFalse();

    /**
     * 특정 연도의 최대 순번 조회
     *
     * 수주번호 생성 시 중복을 방지하기 위해
     * 해당 연도의 가장 큰 순번을 조회합니다.
     * 수주번호 형식: SO-{연도}-{순번} (예: SO-2025-1001)
     *
     * MySQL Native Query를 사용하여 수주번호에서 순번 부분을 추출합니다.
     * SUBSTRING_INDEX 함수를 사용하여 '-' 구분자로 분리된 마지막 부분(순번)을 추출합니다.
     *
     * @param yearPattern 연도 패턴 (예: SO-2025-%)
     * @return 해당 연도의 최대 순번 (없으면 0)
     */
    @Query(value = "SELECT COALESCE(MAX(CAST(SUBSTRING_INDEX(order_number, '-', -1) AS UNSIGNED)), 0) " +
            "FROM sales_orders " +
            "WHERE order_number LIKE :yearPattern AND deleted = false", nativeQuery = true)
    Integer findMaxSequenceByYear(@Param("yearPattern") String yearPattern);
}
