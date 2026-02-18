package com.doosan.erp.auth.controller;

import com.doosan.erp.auth.dto.LoginRequest;
import com.doosan.erp.auth.dto.LoginResponse;
import com.doosan.erp.auth.dto.SignupRequest;
import com.doosan.erp.auth.service.AuthService;
import com.doosan.erp.common.dto.ApiResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 인증 API 컨트롤러
 *
 * 사용자 인증 관련 엔드포인트를 제공합니다.
 * - 회원가입: POST /api/auth/signup
 * - 로그인: POST /api/auth/login
 *
 * 이 컨트롤러의 엔드포인트는 인증 없이 접근 가능합니다.
 * (SecurityConfig의 PUBLIC_URLS에 등록됨)
 */
@Tag(name = "Auth", description = "인증 API")
@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    /**
     * 회원가입 API
     *
     * 새로운 사용자를 등록합니다.
     * 성공 시 생성된 사용자의 ID를 반환합니다.
     *
     * @param request 회원가입 요청 정보 (아이디, 비밀번호, 이름)
     * @return 생성된 사용자 ID
     */
    @Operation(summary = "회원가입", description = "새로운 사용자를 등록합니다")
    @PostMapping("/signup")
    public ResponseEntity<ApiResponse<Long>> signup(@Valid @RequestBody SignupRequest request) {
        Long userId = authService.signup(request);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(ApiResponse.success(userId));
    }

    /**
     * 로그인 API
     *
     * 아이디와 비밀번호로 인증하여 JWT 토큰을 발급합니다.
     * 발급된 토큰은 이후 API 요청 시 Authorization 헤더에 사용됩니다.
     *
     * @param request 로그인 요청 정보 (아이디, 비밀번호)
     * @return JWT 토큰 정보 (accessToken, tokenType, expiresIn)
     */
    @Operation(summary = "로그인", description = "이메일과 비밀번호로 로그인하여 JWT 토큰을 발급받습니다")
    @PostMapping("/login")
    public ResponseEntity<ApiResponse<LoginResponse>> login(@Valid @RequestBody LoginRequest request) {
        LoginResponse response = authService.login(request);
        return ResponseEntity.ok(ApiResponse.success(response));
    }
}
