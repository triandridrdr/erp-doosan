package com.doosan.erp.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Swagger/OpenAPI 설정 클래스
 *
 * REST API 문서를 자동으로 생성하여 제공합니다.
 * Swagger UI를 통해 API를 테스트할 수 있습니다.
 *
 * 접근 URL: /swagger-ui/index.html
 */
@Configuration
public class SwaggerConfig {

    /**
     * OpenAPI 문서 설정
     *
     * API 문서의 기본 정보와 JWT 인증 설정을 정의합니다.
     * - API 제목, 버전, 설명 설정
     * - JWT Bearer 토큰 인증 스키마 설정
     *
     * Swagger UI에서 "Authorize" 버튼을 클릭하여
     * JWT 토큰을 입력하면 인증이 필요한 API를 테스트할 수 있습니다.
     *
     * @return OpenAPI 설정 객체
     */
    @Bean
    public OpenAPI openAPI() {
        return new OpenAPI()
                // API 기본 정보 설정
                .info(new Info()
                        .title("ERP API Template")
                        .version("v1.0.0")
                        .description("Modular Monolith 아키텍처 기반 ERP API 문서")
                        .contact(new Contact()
                                .name("Doosan ERP Team")
                                .email("erp@doosan.com")
                        )
                )
                // 모든 API에 JWT 인증 적용
                .addSecurityItem(new SecurityRequirement().addList("bearerAuth"))
                // JWT 인증 스키마 정의
                .components(new Components()
                        .addSecuritySchemes("bearerAuth",
                                new SecurityScheme()
                                        .type(SecurityScheme.Type.HTTP)  // HTTP 인증 방식
                                        .scheme("bearer")                 // Bearer 토큰 사용
                                        .bearerFormat("JWT")              // JWT 형식
                                        .description("JWT 토큰을 입력하세요 (Bearer prefix 없이)")
                        )
                );
    }
}
