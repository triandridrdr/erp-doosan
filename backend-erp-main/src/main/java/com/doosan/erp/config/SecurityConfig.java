package com.doosan.erp.config;

import com.doosan.erp.security.JwtAuthenticationFilter;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import java.util.List;

/**
 * Spring Security 설정 클래스
 *
 * 애플리케이션의 보안 관련 설정을 담당합니다.
 * - JWT 기반 인증 필터 설정
 * - 세션을 사용하지 않는 STATELESS 방식 채택
 * - CORS(Cross-Origin Resource Sharing) 설정
 * - 비밀번호 암호화 방식 설정
 */
@Configuration
@EnableWebSecurity // Spring Security 활성화
@RequiredArgsConstructor
public class SecurityConfig {

    // JWT 인증을 처리하는 커스텀 필터
    private final JwtAuthenticationFilter jwtAuthenticationFilter;

    // 인증 없이 접근 가능한 URL 패턴 목록
    private static final String[] PUBLIC_URLS = {
            "/api/auth/**", // 인증 API (로그인, 회원가입)
            "/api/docs/**",
            "/api/swagger-ui/**",
            "/api-docs/**",
            "/actuator/**" // Spring Actuator 엔드포인트
    };

    /**
     * Spring Security 필터 체인 설정
     *
     * HTTP 요청에 대한 보안 정책을 정의합니다.
     *
     * @param http HttpSecurity 설정 객체
     * @return 구성된 SecurityFilterChain
     */
    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
                // CSRF 비활성화 (REST API는 세션을 사용하지 않으므로 불필요)
                .csrf(AbstractHttpConfigurer::disable)
                // CORS 설정 적용
                .cors(cors -> cors.configurationSource(corsConfigurationSource()))
                // URL별 접근 권한 설정
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers(PUBLIC_URLS).permitAll() // 공개 URL은 모두 허용
                        .anyRequest().authenticated()) // 그 외는 인증 필요
                // 세션을 사용하지 않음 (JWT 기반 인증)
                .sessionManagement(session -> session
                        .sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                // JWT 필터를 기본 인증 필터 앞에 추가
                .addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class);

        return http.build();
    }

    /**
     * AuthenticationManager Bean 등록
     *
     * UserDetailsServiceAutoConfiguration의 기본 사용자 생성을 방지하고
     * JWT 기반 인증에서 필요한 AuthenticationManager를 제공합니다.
     *
     * @param authConfig AuthenticationConfiguration
     * @return AuthenticationManager 인스턴스
     * @throws Exception 설정 오류 시
     */
    @Bean
    public AuthenticationManager authenticationManager(AuthenticationConfiguration authConfig) throws Exception {
        return authConfig.getAuthenticationManager();
    }

    /**
     * 비밀번호 인코더 Bean 등록
     *
     * BCrypt 해시 알고리즘으로 비밀번호를 암호화합니다.
     * 솔트(salt)를 자동 생성하여 보안성을 높입니다.
     *
     * @return BCryptPasswordEncoder 인스턴스
     */
    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    /**
     * CORS 설정 Bean 등록
     *
     * 다른 도메인에서의 API 요청을 허용합니다.
     * - 로컬, DEV 프론트엔드에서 API 요청 허용
     * - 주요 HTTP 메서드 허용
     * - preflight 요청 캐시: 1시간
     *
     * @return CorsConfigurationSource 객체
     */
    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();

        configuration.setAllowedOrigins(List.of(
            "http://108.137.134.180",
            "http://108.137.134.180:80",
            "http://108.137.134.180:8080",
            "http://127.0.0.1:5173",
            "http://localhost:5173"
        ));

        configuration.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"));
        configuration.setAllowedHeaders(List.of("*"));

        configuration.setAllowCredentials(true);
        configuration.setMaxAge(3600L);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }
}

