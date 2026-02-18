package com.doosan.erp.common.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 에러 응답 DTO
 *
 * 예외 발생 시 클라이언트에게 전달되는 에러 정보를 담습니다.
 *
 * 포함 정보:
 * - errorCode: 에러 코드 (예: ERR-1001)
 * - message: 에러 메시지
 * - timestamp: 에러 발생 시간
 * - path: 에러가 발생한 API 경로
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class ErrorResponse {

    private String errorCode;         // 에러 코드
    private String message;           // 에러 메시지
    private LocalDateTime timestamp;  // 발생 시간
    private String path;              // 요청 경로

    /**
     * ErrorResponse 생성자
     * timestamp는 현재 시간으로 자동 설정됩니다.
     *
     * @param errorCode 에러 코드
     * @param message 에러 메시지
     * @param path 요청 경로
     */
    public ErrorResponse(String errorCode, String message, String path) {
        this.errorCode = errorCode;
        this.message = message;
        this.timestamp = LocalDateTime.now();
        this.path = path;
    }
}
