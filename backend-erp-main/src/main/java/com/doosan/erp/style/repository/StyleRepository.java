package com.doosan.erp.style.repository;

import com.doosan.erp.style.entity.Style;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface StyleRepository extends JpaRepository<Style, Long> {

    @Query("SELECT s FROM Style s " +
            "WHERE (:search IS NULL OR :search = '' " +
            "OR LOWER(s.styleCode) LIKE LOWER(CONCAT('%', :search, '%')) " +
            "OR LOWER(s.styleName) LIKE LOWER(CONCAT('%', :search, '%')) " +
            "OR LOWER(s.season) LIKE LOWER(CONCAT('%', :search, '%'))) " +
            "ORDER BY s.id DESC")
    Page<Style> search(@Param("search") String search, Pageable pageable);
}
