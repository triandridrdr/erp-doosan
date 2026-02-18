package com.doosan.erp.common.exception;

import com.doosan.erp.common.constant.ErrorCode;
import com.doosan.erp.common.dto.ApiResponse;
import com.doosan.erp.common.dto.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

/**
 * 전역 예외 처리 핸들러
 *
 * 애플리케이션에서 발생하는 모든 예외를 잡아서
 * 일관된 형식(ApiResponse)으로 클라이언트에 반환합니다.
 *
 * 처리하는 예외 유형:
 * - BusinessException: 비즈니스 로직 예외
 * - ResourceNotFoundException: 리소스 없음 예외
 * - MethodArgumentNotValidException: 입력값 검증 예외
 * - Exception: 그 외 모든 예외
 */
@Slf4j
@RestControllerAdvice // 모든 컨트롤러에 적용되는 예외 처리기
public class GlobalExceptionHandler {

        /**
         * 비즈니스 예외 처리
         *
         * 비즈니스 규칙 위반 시 발생하는 예외를 처리합니다.
         * 예: 재고 부족, 중복 데이터 등
         *
         * @param ex      발생한 예외
         * @param request HTTP 요청 정보
         * @return 에러 응답
         */
        @ExceptionHandler(BusinessException.class)
        public ResponseEntity<ApiResponse<ErrorResponse>> handleBusinessException(
                        BusinessException ex, HttpServletRequest request) {
                log.error("Business exception occurred: {}", ex.getMessage(), ex);

                ErrorResponse errorResponse = new ErrorResponse(
                                ex.getErrorCode().getCode(),
                                ex.getMessage(),
                                request.getRequestURI());

                return ResponseEntity
                                .status(ex.getErrorCode().getHttpStatus())
                                .body(ApiResponse.error(errorResponse, ex.getMessage()));
        }

        /**
         * 리소스 없음 예외 처리
         *
         * 요청한 리소스를 찾을 수 없을 때 발생하는 예외를 처리합니다.
         * 예: 존재하지 않는 수주 ID로 조회 시
         *
         * @param ex      발생한 예외
         * @param request HTTP 요청 정보
         * @return 404 에러 응답
         */
        @ExceptionHandler(ResourceNotFoundException.class)
        public ResponseEntity<ApiResponse<ErrorResponse>> handleResourceNotFoundException(
                        ResourceNotFoundException ex, HttpServletRequest request) {
                log.error("Resource not found: {}", ex.getMessage(), ex);

                ErrorResponse errorResponse = new ErrorResponse(
                                ex.getErrorCode().getCode(),
                                ex.getMessage(),
                                request.getRequestURI());

                return ResponseEntity
                                .status(HttpStatus.NOT_FOUND)
                                .body(ApiResponse.error(errorResponse, ex.getMessage()));
        }

        /**
         * 입력값 검증 예외 처리
         *
         * Valid 어노테이션 검증 실패 시 발생하는 예외를 처리합니다.
         * 예: 필수값 누락, 형식 오류 등
         *
         * @param ex      발생한 예외
         * @param request HTTP 요청 정보
         * @return 400 에러 응답
         */
        @ExceptionHandler(MethodArgumentNotValidException.class)
        public ResponseEntity<ApiResponse<ErrorResponse>> handleValidationException(
                        MethodArgumentNotValidException ex, HttpServletRequest request) {
                log.error("Validation exception occurred: {}", ex.getMessage());

                // 모든 필드 에러를 하나의 메시지로 결합
                String errorMessage = ex.getBindingResult().getFieldErrors().stream()
                                .map(error -> String.format("%s: %s", error.getField(), error.getDefaultMessage()))
                                .collect(Collectors.joining(", "));

                ErrorResponse errorResponse = new ErrorResponse(
                                ErrorCode.INVALID_INPUT_VALUE.getCode(),
                                errorMessage,
                                request.getRequestURI());

                return ResponseEntity
                                .status(HttpStatus.BAD_REQUEST)
                                .body(ApiResponse.error(errorResponse, "입력값 검증에 실패했습니다"));
        }

        /**
         * 데이터 무결성 위반 예외 처리
         *
         * 데이터베이스 제약 조건 위반 시 발생하는 예외를 처리합니다.
         * 예: 중복 키, 외래 키 제약 위반 등
         *
         * @param ex      발생한 예외
         * @param request HTTP 요청 정보
         * @return 409 에러 응답
         */
        @ExceptionHandler(DataIntegrityViolationException.class)
        public ResponseEntity<ApiResponse<ErrorResponse>> handleDataIntegrityViolation(
                        DataIntegrityViolationException ex, HttpServletRequest request) {
                log.error("Data integrity violation occurred: {}", ex.getMessage());

                // 중복 키 에러인지 확인
                String message = ex.getMessage();
                String errorMessage = "데이터 무결성 제약 조건을 위반했습니다";

                if (message != null && message.contains("duplicate key")) {
                        errorMessage = "이미 존재하는 데이터입니다. 중복된 값을 확인해주세요";
                } else if (message != null && message.contains("foreign key")) {
                        errorMessage = "참조되는 데이터가 존재하지 않습니다";
                }

                ErrorResponse errorResponse = new ErrorResponse(
                                ErrorCode.DUPLICATE_RESOURCE.getCode(),
                                errorMessage,
                                request.getRequestURI());

                return ResponseEntity
                                .status(ErrorCode.DUPLICATE_RESOURCE.getHttpStatus())
                                .body(ApiResponse.error(errorResponse, errorMessage));
        }

        /**
         * 일반 예외 처리 (최종 방어선)
         *
         * 위에서 처리되지 않은 모든 예외를 처리합니다.
         * 예상치 못한 서버 오류 시 사용됩니다.
         *
         * @param ex      발생한 예외
         * @param request HTTP 요청 정보
         * @return 500 에러 응답
         */
        @ExceptionHandler(Exception.class)
        public ResponseEntity<ApiResponse<ErrorResponse>> handleGenericException(
                        Exception ex, HttpServletRequest request) {
                log.error("Unexpected exception occurred: {}", ex.getMessage(), ex);

                ErrorResponse errorResponse = new ErrorResponse(
                                ErrorCode.INTERNAL_SERVER_ERROR.getCode(),
                                ex.getMessage(),
                                request.getRequestURI());

                return ResponseEntity
                                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                                .body(ApiResponse.error(errorResponse, "서버 내부 오류가 발생했습니다"));
        }
}
