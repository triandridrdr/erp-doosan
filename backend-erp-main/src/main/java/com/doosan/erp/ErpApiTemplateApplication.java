package com.doosan.erp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * ERP API 템플릿 애플리케이션의 메인 클래스
 *
 * Spring Boot 애플리케이션의 시작점입니다.
 * SpringBootApplication 어노테이션은 다음 기능을 포함합니다:
 * - Configuration: 설정 클래스 지정
 * - EnableAutoConfiguration: 자동 설정 활성화
 * - ComponentScan: 하위 패키지 컴포넌트 자동 스캔
 */
@SpringBootApplication
public class ErpApiTemplateApplication {

    /**
     * 애플리케이션 진입점
     *
     * @param args 커맨드라인 인자
     */
    public static void main(String[] args) {
        SpringApplication.run(ErpApiTemplateApplication.class, args);
    }

}
