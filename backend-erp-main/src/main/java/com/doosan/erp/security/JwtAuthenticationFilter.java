package com.doosan.erp.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * JWT 인증 필터
 *
 * 모든 HTTP 요청에서 JWT 토큰을 확인하고 인증을 처리하는 필터입니다.
 * OncePerRequestFilter를 상속하여 요청당 한 번만 실행됩니다.
 *
 * 동작 흐름:
 * 1. 요청 헤더에서 Authorization 값 추출
 * 2. "Bearer " 접두사 제거하여 토큰 추출
 * 3. 토큰 유효성 검증
 * 4. 유효하면 SecurityContext에 인증 정보 저장
 * 5. 다음 필터로 요청 전달
 */
@Component
@RequiredArgsConstructor
public class JwtAuthenticationFilter extends OncePerRequestFilter {

    // HTTP 헤더에서 토큰을 담는 키 이름
    private static final String AUTHORIZATION_HEADER = "Authorization";
    // Bearer 토큰 형식의 접두사
    private static final String BEARER_PREFIX = "Bearer ";

    // JWT 토큰 처리를 위한 프로바이더
    private final JwtTokenProvider jwtTokenProvider;

    /**
     * 필터 로직 구현
     *
     * 매 요청마다 JWT 토큰을 확인하고 인증을 처리합니다.
     * 토큰이 유효하면 SecurityContext에 인증 정보를 저장합니다.
     *
     * @param request HTTP 요청
     * @param response HTTP 응답
     * @param filterChain 필터 체인
     */
    @Override
    protected void doFilterInternal(@NonNull HttpServletRequest request,
                                    @NonNull HttpServletResponse response,
                                    @NonNull FilterChain filterChain) throws ServletException, IOException {
        // 1. 요청에서 토큰 추출
        String token = resolveToken(request);

        // 2. 토큰이 있고 유효하면 인증 정보 설정
        if (StringUtils.hasText(token) && jwtTokenProvider.validateToken(token)) {
            Authentication authentication = jwtTokenProvider.getAuthentication(token);
            // SecurityContext에 인증 정보 저장 (이후 컨트롤러에서 사용 가능)
            SecurityContextHolder.getContext().setAuthentication(authentication);
        }

        // 3. 다음 필터로 요청 전달
        filterChain.doFilter(request, response);
    }

    /**
     * HTTP 요청 헤더에서 JWT 토큰 추출
     *
     * Authorization 헤더에서 "Bearer " 접두사를 제거하고
     * 순수한 토큰 문자열만 반환합니다.
     *
     * @param request HTTP 요청
     * @return JWT 토큰 문자열 (없으면 null)
     */
    private String resolveToken(HttpServletRequest request) {
        String bearerToken = request.getHeader(AUTHORIZATION_HEADER);
        // "Bearer {token}" 형식에서 토큰만 추출
        if (StringUtils.hasText(bearerToken) && bearerToken.startsWith(BEARER_PREFIX)) {
            return bearerToken.substring(BEARER_PREFIX.length());
        }
        return null;
    }
}
