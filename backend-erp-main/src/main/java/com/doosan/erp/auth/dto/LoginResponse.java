package com.doosan.erp.auth.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * 로그인 응답 DTO
 *
 * 로그인 성공 시 클라이언트에게 반환되는 JWT 토큰 정보입니다.
 * 클라이언트는 이 토큰을 이후 API 요청 시 Authorization 헤더에 포함합니다.
 *
 * 사용 예시: Authorization: Bearer {accessToken}
 */
@Getter
@Builder
@AllArgsConstructor
public class LoginResponse {

    private String accessToken;  // JWT 토큰
    private String tokenType;    // 토큰 타입 (Bearer)
    private Long expiresIn;      // 만료 시간 (초)

    /**
     * LoginResponse 생성 팩토리 메서드
     *
     * @param accessToken JWT 토큰 문자열
     * @param expiresIn 만료 시간 (초 단위)
     * @return LoginResponse 객체
     */
    public static LoginResponse of(String accessToken, Long expiresIn) {
        return LoginResponse.builder()
                .accessToken(accessToken)
                .tokenType("Bearer")
                .expiresIn(expiresIn)
                .build();
    }
}
