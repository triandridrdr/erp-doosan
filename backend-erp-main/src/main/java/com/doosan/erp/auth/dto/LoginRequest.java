package com.doosan.erp.auth.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

/**
 * 로그인 요청 DTO
 *
 * 로그인 API 요청 시 클라이언트가 전송하는 데이터입니다.
 */
@Getter
@Setter
public class LoginRequest {

    // 사용자 ID (필수)
    @NotBlank(message = "아이디는 필수입니다")
    private String userId;

    // 비밀번호 (필수)
    @NotBlank(message = "비밀번호는 필수입니다")
    private String password;
}
