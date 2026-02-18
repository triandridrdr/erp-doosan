package com.doosan.erp.common.exception;

import com.doosan.erp.common.constant.ErrorCode;

/**
 * 리소스 없음 예외 클래스
 *
 * 요청한 리소스(데이터)를 찾을 수 없을 때 발생시키는 예외입니다.
 * BusinessException을 상속하며, HTTP 404 응답으로 처리됩니다.
 *
 * 사용 예시:
 * - 수주 조회 시 없으면: throw new ResourceNotFoundException(ErrorCode.SALES_ORDER_NOT_FOUND)
 * - 사용자 조회 시 없으면: throw new ResourceNotFoundException("사용자를 찾을 수 없습니다")
 */
public class ResourceNotFoundException extends BusinessException {

    /**
     * 에러 코드로 예외 생성
     */
    public ResourceNotFoundException(ErrorCode errorCode) {
        super(errorCode);
    }

    /**
     * 에러 코드와 커스텀 메시지로 예외 생성
     */
    public ResourceNotFoundException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    /**
     * 메시지만으로 예외 생성
     * 기본 RESOURCE_NOT_FOUND 에러 코드를 사용합니다.
     */
    public ResourceNotFoundException(String message) {
        super(ErrorCode.RESOURCE_NOT_FOUND, message);
    }
}
