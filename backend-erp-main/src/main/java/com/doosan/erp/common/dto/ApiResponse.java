package com.doosan.erp.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 표준 API 응답 래퍼 클래스
 *
 * 모든 API 응답을 통일된 형식으로 감싸서 반환합니다.
 * 클라이언트는 항상 동일한 구조의 응답을 받게 됩니다.
 *
 * 응답 구조:
 * - success: 요청 성공 여부 (true/false)
 * - data: 실제 응답 데이터 (제네릭 타입)
 * - message: 추가 메시지 (선택)
 * - timestamp: 응답 시간
 *
 * @param <T> 응답 데이터의 타입
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)  // null 값은 JSON에서 제외
public class ApiResponse<T> {

    private boolean success;      // 요청 성공 여부
    private T data;               // 응답 데이터
    private String message;       // 메시지
    private LocalDateTime timestamp;  // 응답 시간

    /**
     * 성공 응답 생성 (데이터만)
     *
     * @param data 응답 데이터
     * @return ApiResponse 객체
     */
    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>(true, data, null, LocalDateTime.now());
    }

    /**
     * 성공 응답 생성 (데이터 + 메시지)
     *
     * @param data 응답 데이터
     * @param message 추가 메시지
     * @return ApiResponse 객체
     */
    public static <T> ApiResponse<T> success(T data, String message) {
        return new ApiResponse<>(true, data, message, LocalDateTime.now());
    }

    /**
     * 에러 응답 생성 (메시지만)
     *
     * @param message 에러 메시지
     * @return ApiResponse 객체
     */
    public static <T> ApiResponse<T> error(String message) {
        return new ApiResponse<>(false, null, message, LocalDateTime.now());
    }

    /**
     * 에러 응답 생성 (에러 상세 정보 포함)
     *
     * @param errorData 에러 상세 정보
     * @param message 에러 메시지
     * @return ApiResponse 객체
     */
    public static <T> ApiResponse<T> error(T errorData, String message) {
        return new ApiResponse<>(false, errorData, message, LocalDateTime.now());
    }
}
