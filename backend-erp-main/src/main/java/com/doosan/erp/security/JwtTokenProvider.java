package com.doosan.erp.security;

import io.jsonwebtoken.*;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.util.Collections;
import java.util.Date;

/**
 * JWT 토큰 제공자 클래스
 *
 * JWT(JSON Web Token) 생성, 검증, 파싱을 담당합니다.
 * 로그인 성공 시 토큰을 생성하고, 요청 시 토큰을 검증합니다.
 *
 * JWT 구조: Header.Payload.Signature
 * - Header: 토큰 타입과 암호화 알고리즘 정보
 * - Payload: 사용자 ID, 권한, 만료시간 등의 클레임
 * - Signature: 토큰 무결성 검증용 서명
 */
@Slf4j
@Component
public class JwtTokenProvider {

    // application.yml에서 설정된 비밀키 (Base64 인코딩)
    @Value("${jwt.secret}")
    private String secretKey;

    // 토큰 만료 시간 (밀리초 단위)
    @Value("${jwt.expiration}")
    private long expiration;

    // HMAC-SHA 알고리즘용 비밀키 객체
    private SecretKey key;

    /**
     * 빈 초기화 시 비밀키를 디코딩하여 SecretKey 객체 생성
     * PostConstruct: 의존성 주입 완료 후 실행
     */
    @PostConstruct
    public void init() {
        byte[] keyBytes = Decoders.BASE64.decode(secretKey);
        this.key = Keys.hmacShaKeyFor(keyBytes);
    }

    /**
     * JWT 토큰 생성
     *
     * 사용자 ID와 권한 정보를 담은 토큰을 생성합니다.
     *
     * @param userId 사용자 ID (토큰의 subject가 됨)
     * @param role 사용자 권한 (USER, ADMIN 등)
     * @return 생성된 JWT 토큰 문자열
     */
    public String createToken(String userId, String role) {
        Date now = new Date();
        Date expiryDate = new Date(now.getTime() + expiration);

        return Jwts.builder()
                .subject(userId)           // 토큰 주체 (사용자 ID)
                .claim("role", role)       // 커스텀 클레임 (권한)
                .issuedAt(now)             // 토큰 발급 시간
                .expiration(expiryDate)    // 토큰 만료 시간
                .signWith(key)             // 서명
                .compact();                // 토큰 문자열로 변환
    }

    /**
     * 토큰에서 인증 정보 추출
     *
     * JWT 토큰을 파싱하여 Spring Security의 Authentication 객체를 생성합니다.
     * SecurityContext에 저장되어 컨트롤러에서 인증 정보로 사용됩니다.
     *
     * @param token JWT 토큰 문자열
     * @return Authentication 인증 객체
     */
    public Authentication getAuthentication(String token) {
        // 토큰 파싱하여 클레임 추출
        Claims claims = Jwts.parser()
                .verifyWith(key)
                .build()
                .parseSignedClaims(token)
                .getPayload();

        // 권한 정보 생성 (ROLE_ 접두사 추가)
        String role = claims.get("role", String.class);
        SimpleGrantedAuthority authority = new SimpleGrantedAuthority("ROLE_" + role);

        // Spring Security User 객체 생성
        User principal = new User(claims.getSubject(), "", Collections.singletonList(authority));
        return new UsernamePasswordAuthenticationToken(principal, token, Collections.singletonList(authority));
    }

    /**
     * 토큰 유효성 검증
     *
     * 토큰의 서명과 만료시간을 검증합니다.
     *
     * @param token 검증할 JWT 토큰
     * @return 유효하면 true, 아니면 false
     */
    public boolean validateToken(String token) {
        try {
            Jwts.parser()
                    .verifyWith(key)
                    .build()
                    .parseSignedClaims(token);
            return true;
        } catch (SecurityException | MalformedJwtException e) {
            log.error("잘못된 JWT 서명입니다.");
        } catch (ExpiredJwtException e) {
            log.error("만료된 JWT 토큰입니다.");
        } catch (UnsupportedJwtException e) {
            log.error("지원되지 않는 JWT 토큰입니다.");
        } catch (IllegalArgumentException e) {
            log.error("JWT 토큰이 잘못되었습니다.");
        }
        return false;
    }

    /**
     * 토큰 만료 시간 반환 (밀리초)
     */
    public long getExpiration() {
        return expiration;
    }
}
