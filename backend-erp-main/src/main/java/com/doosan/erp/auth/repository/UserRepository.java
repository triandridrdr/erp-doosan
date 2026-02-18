package com.doosan.erp.auth.repository;

import com.doosan.erp.auth.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * 사용자 Repository 인터페이스
 *
 * JpaRepository를 상속하여 기본 CRUD와 페이징을 제공받고,
 * 추가로 사용자 조회를 위한 커스텀 메서드를 정의합니다.
 */
public interface UserRepository extends JpaRepository<User, Long> {

    /**
     * 사용자 ID로 사용자 조회
     *
     * @param userId 사용자 ID
     * @return Optional로 감싼 User 객체
     */
    Optional<User> findByUserId(String userId);

    /**
     * 사용자 ID 존재 여부 확인
     * 회원가입 시 중복 검사에 사용됩니다.
     *
     * @param userId 사용자 ID
     * @return 존재하면 true
     */
    boolean existsByUserId(String userId);
}
