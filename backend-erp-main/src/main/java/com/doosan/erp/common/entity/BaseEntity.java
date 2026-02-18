package com.doosan.erp.common.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.LocalDateTime;

/**
 * 공통 엔티티 베이스 클래스
 *
 * 모든 도메인 엔티티가 상속받아 사용하는 추상 클래스입니다.
 *
 * 제공 기능:
 * - id: 자동 생성되는 기본키
 * - createdAt/updatedAt: 생성/수정 시간 자동 기록
 * - createdBy/updatedBy: 생성/수정자 자동 기록
 * - deleted/deletedAt: Soft Delete 지원
 *
 * 사용 예시: User, SalesOrder, Stock 등 모든 엔티티가 상속
 */
@MappedSuperclass  // JPA에서 상속용 부모 클래스 지정 (테이블 생성 안 됨)
@EntityListeners(AuditingEntityListener.class)  // JPA Auditing 리스너 적용
@Getter
@Setter
public abstract class BaseEntity {

    // 기본키 (자동 증가)
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // 생성 시간 (JPA Auditing이 자동 설정)
    @CreatedDate
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    // 수정 시간 (JPA Auditing이 자동 갱신)
    @LastModifiedDate
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    // 생성자 (JPA Auditing이 자동 설정)
    @CreatedBy
    @Column(name = "created_by", updatable = false, length = 50)
    private String createdBy;

    // 수정자 (JPA Auditing이 자동 갱신)
    @LastModifiedBy
    @Column(name = "updated_by", length = 50)
    private String updatedBy;

    // Soft Delete 여부 (true면 삭제된 데이터)
    @Column(name = "deleted", nullable = false)
    private Boolean deleted = false;

    // 삭제 시간
    @Column(name = "deleted_at")
    private LocalDateTime deletedAt;

    /**
     * Soft Delete 처리
     *
     * 실제로 데이터를 삭제하지 않고 deleted 플래그만 변경합니다.
     * 조회 시 deleted=false 조건으로 필터링하여 삭제된 데이터를 제외합니다.
     */
    public void softDelete() {
        this.deleted = true;
        this.deletedAt = LocalDateTime.now();
    }

    /**
     * Soft Delete 복구
     *
     * 삭제된 데이터를 다시 활성화합니다.
     */
    public void restore() {
        this.deleted = false;
        this.deletedAt = null;
    }
}
