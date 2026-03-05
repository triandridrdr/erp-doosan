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
    INTERNAL_SERVER_ERROR("ERR-1000", "An internal server error occurred", HttpStatus.INTERNAL_SERVER_ERROR),
    INVALID_INPUT_VALUE("ERR-1001", "Invalid input value", HttpStatus.BAD_REQUEST),
    RESOURCE_NOT_FOUND("ERR-1002", "Requested resource not found", HttpStatus.NOT_FOUND),
    METHOD_NOT_ALLOWED("ERR-1003", "HTTP method not allowed", HttpStatus.METHOD_NOT_ALLOWED),
    UNAUTHORIZED("ERR-1004", "Authentication required", HttpStatus.UNAUTHORIZED),
    FORBIDDEN("ERR-1005", "Access denied", HttpStatus.FORBIDDEN),
    DUPLICATE_RESOURCE("ERR-1006", "Resource already exists", HttpStatus.CONFLICT),

    // ==================== 인증 도메인 에러 (1100번대) ====================
    USER_ID_ALREADY_EXISTS("ERR-1100", "User ID already exists", HttpStatus.CONFLICT),
    INVALID_CREDENTIALS("ERR-1101", "Invalid username or password", HttpStatus.UNAUTHORIZED),
    USER_NOT_FOUND("ERR-1102", "User not found", HttpStatus.NOT_FOUND),

    // ==================== 회계 도메인 에러 (2000번대) ====================
    JOURNAL_ENTRY_NOT_FOUND("ERR-2001", "Journal entry not found", HttpStatus.NOT_FOUND),
    JOURNAL_ENTRY_ALREADY_POSTED("ERR-2002", "Journal entry already posted", HttpStatus.BAD_REQUEST),
    INVALID_JOURNAL_ENTRY("ERR-2003", "Debit and credit do not match", HttpStatus.BAD_REQUEST),

    // ==================== 판매 도메인 에러 (3000번대) ====================
    SALES_ORDER_NOT_FOUND("ERR-3001", "Sales order not found", HttpStatus.NOT_FOUND),
    SALES_ORDER_ALREADY_CONFIRMED("ERR-3002", "Sales order already confirmed", HttpStatus.BAD_REQUEST),
    INVALID_SALES_ORDER("ERR-3003", "Invalid sales order", HttpStatus.BAD_REQUEST),

    // ==================== 재고 도메인 에러 (4000번대) ====================
    ITEM_NOT_FOUND("ERR-4001", "Item not found", HttpStatus.NOT_FOUND),
    STOCK_NOT_FOUND("ERR-4002", "Stock not found", HttpStatus.NOT_FOUND),
    INSUFFICIENT_STOCK("ERR-4003", "Insufficient stock", HttpStatus.BAD_REQUEST),
    INVALID_STOCK_QUANTITY("ERR-4004", "Invalid stock quantity", HttpStatus.BAD_REQUEST),

    // ==================== OCR 도메인 에러 (5000번대) ====================
    OCR_PROCESSING_FAILED("ERR-5001", "OCR processing failed", HttpStatus.INTERNAL_SERVER_ERROR),
    OCR_INVALID_FILE("ERR-5002", "Unsupported file type", HttpStatus.BAD_REQUEST),
    OCR_FILE_EMPTY("ERR-5003", "File is empty", HttpStatus.BAD_REQUEST);

    private final String code;           // 에러 코드 (예: ERR-1001)
    private final String message;        // 에러 메시지
    private final HttpStatus httpStatus; // HTTP 상태 코드
}
