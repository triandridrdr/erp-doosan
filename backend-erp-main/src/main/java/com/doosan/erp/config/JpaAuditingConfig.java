package com.doosan.erp.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.AuditorAware;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;

import java.util.Optional;

/**
 * JPA Auditing 설정 클래스
 *
 * 엔티티의 생성일, 수정일, 생성자, 수정자를 자동으로 관리합니다.
 * BaseEntity의 createdAt, updatedAt, createdBy, updatedBy 필드에 적용됩니다.
 *
 * 사용 예시:
 * - 엔티티 생성 시: createdAt, createdBy 자동 설정
 * - 엔티티 수정 시: updatedAt, updatedBy 자동 갱신
 */
@Configuration
@EnableJpaAuditing  // JPA Auditing 기능 활성화
public class JpaAuditingConfig {

    /**
     * 현재 사용자 정보를 제공하는 Bean
     *
     * 엔티티 저장 시 createdBy, updatedBy 필드에 자동으로 설정됩니다.
     * 현재는 "system"으로 고정되어 있으며,
     * Spring Security 연동 시 인증된 사용자 ID로 변경 가능합니다.
     *
     * @return AuditorAware 구현체
     */
    @Bean
    public AuditorAware<String> auditorProvider() {
        // TODO: Spring Security 연동 시 SecurityContextHolder에서 사용자 정보 추출
        return () -> Optional.of("system");
    }
}
