package com.doosan.erp.common.exception;

import com.doosan.erp.common.constant.ErrorCode;
import lombok.Getter;

/**
 * 비즈니스 로직 예외 클래스
 *
 * 비즈니스 규칙 위반 시 발생시키는 커스텀 예외입니다.
 * RuntimeException을 상속하여 Unchecked Exception으로 동작합니다.
 *
 * 사용 예시:
 * - 재고 부족: throw new BusinessException(ErrorCode.INSUFFICIENT_STOCK)
 * - 중복 데이터: throw new BusinessException(ErrorCode.DUPLICATE_RESOURCE, "이미 존재합니다")
 *
 * GlobalExceptionHandler에서 이 예외를 잡아 적절한 HTTP 응답으로 변환합니다.
 */
@Getter
public class BusinessException extends RuntimeException {

    // 예외와 연관된 에러 코드
    private final ErrorCode errorCode;

    /**
     * 에러 코드만으로 예외 생성
     * 에러 코드에 정의된 기본 메시지를 사용합니다.
     */
    public BusinessException(ErrorCode errorCode) {
        super(errorCode.getMessage());
        this.errorCode = errorCode;
    }

    /**
     * 에러 코드와 커스텀 메시지로 예외 생성
     * 상황에 맞는 상세한 메시지를 지정할 수 있습니다.
     */
    public BusinessException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * 에러 코드와 원인 예외로 예외 생성
     * 다른 예외를 래핑할 때 사용합니다.
     */
    public BusinessException(ErrorCode errorCode, Throwable cause) {
        super(errorCode.getMessage(), cause);
        this.errorCode = errorCode;
    }
}
