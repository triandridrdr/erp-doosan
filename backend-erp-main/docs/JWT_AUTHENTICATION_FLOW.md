# JWT 토큰 인증 플로우

## 전체 인증 플로우

### 1. 회원가입/로그인 단계

```
사용자 → AuthController → AuthService → JwtTokenProvider
```

#### 회원가입
**위치**: `src/main/java/com/doosan/erp/auth/controller/AuthController.java:28`

1. `POST /api/auth/signup` 요청
2. `AuthService.signup()` 실행:
   - userId 중복 체크
   - 비밀번호 BCrypt 암호화
   - User 엔티티 저장

**요청 예시**:
```json
POST /api/auth/signup
{
  "userId": "testuser",
  "password": "password123",
  "name": "홍길동"
}
```

#### 로그인
**위치**: `src/main/java/com/doosan/erp/auth/controller/AuthController.java:36`

1. `POST /api/auth/login` 요청 (userId + password)
2. `AuthService.login()` 실행:
   - userId로 사용자 조회
   - 비밀번호 검증
   - JWT 토큰 생성
3. 토큰 반환 (Bearer 토큰, 만료시간 포함)

**요청/응답 예시**:
```json
POST /api/auth/login
{
  "userId": "testuser",
  "password": "password123"
}

Response:
{
  "success": true,
  "data": {
    "accessToken": "eyJhbGciOiJIUzI1NiJ9...",
    "tokenType": "Bearer",
    "expiresIn": 3600
  }
}
```

---

### 2. JWT 토큰 생성

**위치**: `src/main/java/com/doosan/erp/security/JwtTokenProvider.java:37-48`

```java
public String createToken(String userId, String role) {
    Date now = new Date();
    Date expiryDate = new Date(now.getTime() + expiration);

    return Jwts.builder()
        .subject(userId)           // 토큰 주체 (사용자 ID)
        .claim("role", role)       // 역할 정보 (USER, ADMIN)
        .issuedAt(now)             // 발급 시간
        .expiration(expiryDate)    // 만료 시간 (1시간 후)
        .signWith(key)             // HMAC SHA-256 서명
        .compact();
}
```

**토큰 구조**:
```
eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0dXNlciIsInJvbGUiOiJVU0VSIiwiaWF0IjoxNzM0MjM0NTY3LCJleHAiOjE3MzQyMzgxNjd9.signature

Header:
{
  "alg": "HS256"
}

Payload:
{
  "sub": "testuser",
  "role": "USER",
  "iat": 1734234567,
  "exp": 1734238167
}
```

---

### 3. API 요청 시 인증 플로우

```
클라이언트 → JwtAuthenticationFilter → Controller
```

#### 3-1. JwtAuthenticationFilter 동작
**위치**: `src/main/java/com/doosan/erp/security/JwtAuthenticationFilter.java:26-37`

모든 HTTP 요청마다 실행되는 필터:

```java
protected void doFilterInternal(HttpServletRequest request, ...) {
    // 1. Authorization 헤더에서 토큰 추출
    String token = resolveToken(request);

    // 2. 토큰 검증
    if (StringUtils.hasText(token) && jwtTokenProvider.validateToken(token)) {
        // 3. 토큰에서 인증 정보 추출
        Authentication authentication = jwtTokenProvider.getAuthentication(token);

        // 4. SecurityContext에 인증 정보 저장
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    // 5. 다음 필터로 이동
    filterChain.doFilter(request, response);
}
```

#### 3-2. 토큰 추출
**위치**: `src/main/java/com/doosan/erp/security/JwtAuthenticationFilter.java:39-44`

```java
private String resolveToken(HttpServletRequest request) {
    String bearerToken = request.getHeader("Authorization");
    // "Bearer eyJhbGciOiJ..." → "eyJhbGciOiJ..."
    if (bearerToken != null && bearerToken.startsWith("Bearer ")) {
        return bearerToken.substring(7); // "Bearer " 제거
    }
    return null;
}
```

#### 3-3. 토큰 검증
**위치**: `src/main/java/com/doosan/erp/security/JwtTokenProvider.java:64-81`

```java
public boolean validateToken(String token) {
    try {
        Jwts.parser()
            .verifyWith(key)      // 서명 검증
            .build()
            .parseSignedClaims(token);
        return true;
    } catch (ExpiredJwtException e) {
        log.error("만료된 JWT 토큰");  // 1시간 경과
    } catch (MalformedJwtException e) {
        log.error("잘못된 JWT 서명");  // 위조된 토큰
    }
    return false;
}
```

#### 3-4. 인증 정보 추출
**위치**: `src/main/java/com/doosan/erp/security/JwtTokenProvider.java:50-62`

```java
public Authentication getAuthentication(String token) {
    Claims claims = Jwts.parser()
        .verifyWith(key)
        .build()
        .parseSignedClaims(token)
        .getPayload();

    String userId = claims.getSubject();      // "testuser"
    String role = claims.get("role");         // "USER" or "ADMIN"

    // Spring Security 인증 객체 생성
    return new UsernamePasswordAuthenticationToken(
        principal,  // userId
        token,
        authorities // ROLE_USER or ROLE_ADMIN
    );
}
```

---

### 4. 인증 여부 체크

**위치**: `src/main/java/com/doosan/erp/config/SecurityConfig.java:46-48`

```java
.authorizeHttpRequests(auth -> auth
    .requestMatchers(PUBLIC_URLS).permitAll()  // 인증 불필요
    .anyRequest().authenticated())             // 나머지는 인증 필수
```

#### 인증 불필요 (PUBLIC_URLS)
- `/api/auth/**` (로그인, 회원가입)
- `/api/docs`, `/swagger-ui/**`, `/v3/api-docs/**`
- `/actuator/**`

#### 인증 필수
- `/api/sales/**`
- `/api/inventory/**`
- `/api/accounting/**`
- 기타 모든 API

---

### 5. 실제 요청 예시

#### ✅ 성공 케이스
```http
GET /api/sales/orders
Authorization: Bearer eyJhbGciOiJIUzI1NiJ9...

→ JwtAuthenticationFilter에서 토큰 검증 ✓
→ SecurityContext에 인증 정보 저장 ✓
→ Controller 실행 ✓
→ 200 OK
```

#### ❌ 실패 케이스 1: 토큰 없음
```http
GET /api/sales/orders
(Authorization 헤더 없음)

→ JwtAuthenticationFilter: 토큰 없음
→ SecurityContext: 인증 정보 없음
→ SecurityConfig: .anyRequest().authenticated() 위반
→ 401 Unauthorized
```

#### ❌ 실패 케이스 2: 잘못된 토큰
```http
GET /api/sales/orders
Authorization: Bearer invalid-token

→ JwtAuthenticationFilter: 토큰 검증 실패
→ SecurityContext: 인증 정보 없음
→ 401 Unauthorized
```

#### ❌ 실패 케이스 3: 만료된 토큰
```http
GET /api/sales/orders
Authorization: Bearer eyJhbGciOiJIUzI1NiJ9... (1시간 경과)

→ JwtAuthenticationFilter: ExpiredJwtException 발생
→ 401 Unauthorized
```

---

## 흐름도

```
┌─────────────┐
│  클라이언트   │
└──────┬──────┘
       │ 1. POST /api/auth/login
       │    {userId, password}
       ▼
┌─────────────────┐
│ AuthController  │
└────────┬────────┘
         │ 2. login()
         ▼
┌─────────────────┐
│  AuthService    │───→ 비밀번호 검증
└────────┬────────┘
         │ 3. createToken()
         ▼
┌──────────────────┐
│ JwtTokenProvider │───→ JWT 생성
└────────┬─────────┘
         │
         │ 4. 토큰 반환
         ▼
┌─────────────┐
│  클라이언트   │ (토큰 저장)
└──────┬──────┘
       │ 5. GET /api/sales/orders
       │    Authorization: Bearer {token}
       ▼
┌──────────────────────┐
│ JwtAuthentication    │───→ 토큰 추출 & 검증
│ Filter               │
└──────┬───────────────┘
       │ 6. SecurityContext에
       │    인증 정보 저장
       ▼
┌──────────────────┐
│ SecurityConfig   │───→ 권한 체크
└────────┬─────────┘
         │ 7. 인증 OK
         ▼
┌──────────────────┐
│   Controller     │───→ API 실행
└──────────────────┘
```

---

## 핵심 요약

1. **토큰 발급**: 로그인 시 userId + role을 담은 JWT 생성
2. **토큰 검증**: 매 요청마다 JwtAuthenticationFilter가 자동으로 검증
3. **인증 정보 저장**: SecurityContext에 저장되어 Controller에서 사용 가능
4. **권한 체크**: SecurityConfig에서 URL별 접근 권한 제어

---

## 주요 파일 목록

| 파일 | 역할 |
|------|------|
| `auth/entity/User.java` | 사용자 엔티티 |
| `auth/repository/UserRepository.java` | 사용자 Repository |
| `auth/dto/LoginRequest.java` | 로그인 요청 DTO |
| `auth/dto/LoginResponse.java` | 로그인 응답 DTO |
| `auth/dto/SignupRequest.java` | 회원가입 요청 DTO |
| `auth/service/AuthService.java` | 인증 서비스 |
| `auth/controller/AuthController.java` | 인증 API 컨트롤러 |
| `security/JwtTokenProvider.java` | JWT 토큰 생성/검증 |
| `security/JwtAuthenticationFilter.java` | JWT 인증 필터 |
| `config/SecurityConfig.java` | Spring Security 설정 |

---

## 설정 파일

### application.yml
```yaml
jwt:
  secret: ${JWT_SECRET:dGhpc2lzYXZlcnlzZWN1cmVzZWNyZXRrZXlmb3Jqd3R0b2tlbmdlbmVyYXRpb24xMjM0NTY3ODkw}
  expiration: 3600000  # 1시간 (밀리초)
```

---

## 보안 고려사항

### 현재 구현 방식: Stateless JWT
- **장점**: DB 조회 없어서 빠름, 확장성 좋음
- **단점**: 탈퇴/차단된 유저도 토큰 만료 전까지 접근 가능
- **적합**: 대부분의 일반적인 API 서비스

### 주의사항
- ⚠️ JWT 시크릿 키는 **256 bits 이상** 필요
- ⚠️ 프로덕션 환경에서는 환경 변수로 시크릿 키 설정 필수
- ⚠️ 토큰 만료 시간 적절히 설정 (기본 1시간)
- ⚠️ HTTPS 사용 필수 (토큰 탈취 방지)

---

## Swagger UI 사용 방법

1. **애플리케이션 실행**
   ```bash
   ./gradlew bootRun
   ```

2. **Swagger UI 접속**
   ```
   http://localhost:8080/api/docs
   ```

3. **JWT 토큰 획득**
   - Swagger에서 `POST /api/auth/login` 실행
   - 응답에서 `accessToken` 복사

4. **토큰 설정**
   - Swagger 우측 상단 **🔓 Authorize** 버튼 클릭
   - `bearerAuth` 필드에 토큰 입력 (Bearer 없이)
   - **Authorize** 클릭

5. **인증된 API 호출**
   - 이제 모든 API 요청에 자동으로 `Authorization: Bearer {token}` 헤더 추가됨
