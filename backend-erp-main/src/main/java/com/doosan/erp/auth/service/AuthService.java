package com.doosan.erp.auth.service;

import com.doosan.erp.auth.dto.LoginRequest;
import com.doosan.erp.auth.dto.LoginResponse;
import com.doosan.erp.auth.dto.SignupRequest;
import com.doosan.erp.auth.entity.User;
import com.doosan.erp.auth.repository.UserRepository;
import com.doosan.erp.common.constant.ErrorCode;
import com.doosan.erp.common.exception.BusinessException;
import com.doosan.erp.security.JwtTokenProvider;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 인증 서비스
 *
 * 회원가입과 로그인 비즈니스 로직을 처리합니다.
 * - 회원가입: 아이디 중복 검사 후 비밀번호 암호화하여 저장
 * - 로그인: 자격 증명 확인 후 JWT 토큰 발급
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)  // 기본적으로 읽기 전용 트랜잭션
public class AuthService {

    private final UserRepository userRepository;      // 사용자 데이터 접근
    private final PasswordEncoder passwordEncoder;   // 비밀번호 암호화
    private final JwtTokenProvider jwtTokenProvider; // JWT 토큰 생성

    /**
     * 회원가입 처리
     *
     * 1. 아이디 중복 검사
     * 2. 비밀번호 BCrypt 암호화
     * 3. 사용자 정보 저장
     *
     * @param request 회원가입 요청 DTO
     * @return 생성된 사용자 ID
     * @throws BusinessException 아이디 중복 시
     */
    @Transactional  // 쓰기 작업이므로 readOnly = false
    public Long signup(SignupRequest request) {
        // 아이디 중복 검사
        if (userRepository.existsByUserId(request.getUserId())) {
            throw new BusinessException(ErrorCode.USER_ID_ALREADY_EXISTS);
        }

        // 사용자 엔티티 생성 (비밀번호 암호화)
        User user = User.builder()
                .userId(request.getUserId())
                .password(passwordEncoder.encode(request.getPassword()))
                .name(request.getName())
                .role(User.Role.USER)  // 기본 권한: USER
                .build();

        return userRepository.save(user).getId();
    }

    /**
     * 로그인 처리
     *
     * 1. 사용자 조회
     * 2. 비밀번호 검증
     * 3. JWT 토큰 생성 및 반환
     *
     * @param request 로그인 요청 DTO
     * @return JWT 토큰 정보
     * @throws BusinessException 아이디/비밀번호 불일치 시
     */
    public LoginResponse login(LoginRequest request) {
        // 사용자 조회 (없으면 예외)
        User user = userRepository.findByUserId(request.getUserId())
                .orElseThrow(() -> new BusinessException(ErrorCode.INVALID_CREDENTIALS));

        // 비밀번호 검증 (불일치 시 예외)
        if (!passwordEncoder.matches(request.getPassword(), user.getPassword())) {
            throw new BusinessException(ErrorCode.INVALID_CREDENTIALS);
        }

        // JWT 토큰 생성 후 반환
        String token = jwtTokenProvider.createToken(user.getUserId(), user.getRole().name());
        return LoginResponse.of(token, jwtTokenProvider.getExpiration() / 1000);
    }
}
