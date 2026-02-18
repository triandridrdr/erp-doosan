package com.doosan.erp.auth.entity;

import com.doosan.erp.common.entity.BaseEntity;
import jakarta.persistence.*;
import lombok.*;

/**
 * 사용자 엔티티
 *
 * 시스템 사용자 정보를 저장하는 엔티티입니다.
 * BaseEntity를 상속하여 생성일, 수정일 등을 자동 관리합니다.
 *
 * 테이블: users
 */
@Entity
@Table(name = "users")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class User extends BaseEntity {

    // 로그인용 사용자 ID (고유값)
    @Column(nullable = false, unique = true, length = 50)
    private String userId;

    // 암호화된 비밀번호 (BCrypt)
    @Column(nullable = false)
    private String password;

    // 사용자 이름
    @Column(nullable = false, length = 50)
    private String name;

    // 사용자 권한 (USER 또는 ADMIN)
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    @Builder.Default
    private Role role = Role.USER;

    /**
     * 사용자 권한 열거형
     * - USER: 일반 사용자
     * - ADMIN: 관리자
     */
    public enum Role {
        USER, ADMIN
    }
}
