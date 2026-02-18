package com.doosan.erp.auth.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;

/**
 * 회원가입 요청 DTO
 *
 * 회원가입 API 요청 시 클라이언트가 전송하는 데이터입니다.
 * Bean Validation을 통해 입력값을 검증합니다.
 */
@Getter
@Setter
public class SignupRequest {

    // 사용자 ID: 필수, 4~20자, 영문+숫자만 허용
    @NotBlank(message = "아이디는 필수입니다")
    @Size(min = 4, max = 20, message = "아이디는 4~20자여야 합니다")
    @Pattern(regexp = "^[a-zA-Z0-9]+$", message = "아이디는 영문과 숫자만 사용 가능합니다")
    private String userId;

    // 비밀번호: 필수, 최소 6자
    @NotBlank(message = "비밀번호는 필수입니다")
    @Size(min = 6, message = "비밀번호는 최소 6자 이상이어야 합니다")
    private String password;

    // 이름: 필수, 최대 50자
    @NotBlank(message = "이름은 필수입니다")
    @Size(max = 50, message = "이름은 50자를 초과할 수 없습니다")
    private String name;
}
