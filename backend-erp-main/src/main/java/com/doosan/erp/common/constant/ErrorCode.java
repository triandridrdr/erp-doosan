package com.doosan.erp.common.constant;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;

/**
 * 에러 코드 열거형
 *
 * 애플리케이션에서 발생할 수 있는 모든 에러 코드를 정의합니다.
 * 각 에러 코드는 코드값, 메시지, HTTP 상태를 포함합니다.
 *
 * 에러 코드 체계:
 * - 1000번대: 공통 에러
 * - 1100번대: 인증 도메인 에러
 * - 2000번대: 회계 도메인 에러
 * - 3000번대: 판매 도메인 에러
 * - 4000번대: 재고 도메인 에러
 *
 * 사용 예시: throw new BusinessException(ErrorCode.INSUFFICIENT_STOCK)
 */
@Getter
@RequiredArgsConstructor
public enum ErrorCode {

    // ==================== 공통 에러 (1000번대) ====================
    INTERNAL_SERVER_ERROR("ERR-1000", "서버 내부 오류가 발생했습니다", HttpStatus.INTERNAL_SERVER_ERROR),
    INVALID_INPUT_VALUE("ERR-1001", "잘못된 입력 값입니다", HttpStatus.BAD_REQUEST),
    RESOURCE_NOT_FOUND("ERR-1002", "요청한 리소스를 찾을 수 없습니다", HttpStatus.NOT_FOUND),
    METHOD_NOT_ALLOWED("ERR-1003", "허용되지 않은 HTTP 메서드입니다", HttpStatus.METHOD_NOT_ALLOWED),
    UNAUTHORIZED("ERR-1004", "인증이 필요합니다", HttpStatus.UNAUTHORIZED),
    FORBIDDEN("ERR-1005", "권한이 없습니다", HttpStatus.FORBIDDEN),
    DUPLICATE_RESOURCE("ERR-1006", "이미 존재하는 리소스입니다", HttpStatus.CONFLICT),

    // ==================== 인증 도메인 에러 (1100번대) ====================
    USER_ID_ALREADY_EXISTS("ERR-1100", "이미 사용 중인 아이디입니다", HttpStatus.CONFLICT),
    INVALID_CREDENTIALS("ERR-1101", "아이디 또는 비밀번호가 올바르지 않습니다", HttpStatus.UNAUTHORIZED),
    USER_NOT_FOUND("ERR-1102", "사용자를 찾을 수 없습니다", HttpStatus.NOT_FOUND),

    // ==================== 회계 도메인 에러 (2000번대) ====================
    JOURNAL_ENTRY_NOT_FOUND("ERR-2001", "회계전표를 찾을 수 없습니다", HttpStatus.NOT_FOUND),
    JOURNAL_ENTRY_ALREADY_POSTED("ERR-2002", "이미 전기된 전표입니다", HttpStatus.BAD_REQUEST),
    INVALID_JOURNAL_ENTRY("ERR-2003", "차변과 대변이 일치하지 않습니다", HttpStatus.BAD_REQUEST),

    // ==================== 판매 도메인 에러 (3000번대) ====================
    SALES_ORDER_NOT_FOUND("ERR-3001", "수주를 찾을 수 없습니다", HttpStatus.NOT_FOUND),
    SALES_ORDER_ALREADY_CONFIRMED("ERR-3002", "이미 확정된 수주입니다", HttpStatus.BAD_REQUEST),
    INVALID_SALES_ORDER("ERR-3003", "유효하지 않은 수주입니다", HttpStatus.BAD_REQUEST),

    // ==================== 재고 도메인 에러 (4000번대) ====================
    ITEM_NOT_FOUND("ERR-4001", "품목을 찾을 수 없습니다", HttpStatus.NOT_FOUND),
    STOCK_NOT_FOUND("ERR-4002", "재고를 찾을 수 없습니다", HttpStatus.NOT_FOUND),
    INSUFFICIENT_STOCK("ERR-4003", "재고가 부족합니다", HttpStatus.BAD_REQUEST),
    INVALID_STOCK_QUANTITY("ERR-4004", "유효하지 않은 재고 수량입니다", HttpStatus.BAD_REQUEST),

    // ==================== OCR 도메인 에러 (5000번대) ====================
    OCR_PROCESSING_FAILED("ERR-5001", "OCR 처리 중 오류가 발생했습니다", HttpStatus.INTERNAL_SERVER_ERROR),
    OCR_INVALID_FILE("ERR-5002", "지원하지 않는 파일 형식입니다", HttpStatus.BAD_REQUEST),
    OCR_FILE_EMPTY("ERR-5003", "파일이 비어있습니다", HttpStatus.BAD_REQUEST);

    private final String code;           // 에러 코드 (예: ERR-1001)
    private final String message;        // 에러 메시지
    private final HttpStatus httpStatus; // HTTP 상태 코드
}
