package com.sparta.logistics.gateway.security.filter;

import com.sparta.logistics.gateway.jwt.JwtUtil;
import com.sparta.logistics.gateway.security.service.BlackListCheck;
import io.jsonwebtoken.Claims;
import java.util.Collections;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.http.HttpCookie;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextImpl;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@Slf4j(topic = "PreAuthWebFilter")
public class PreAuthWebFilter implements WebFilter, Ordered {

    private final JwtUtil jwtUtil;
    private final BlackListCheck blackListCheck;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        // 토큰 원문은 로그에 남기지 않는다
        String authHeader = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);

        if (authHeader == null) {
            return chain.filter(exchange);
        }

        String accessToken = jwtUtil.removeBearer(authHeader);

        // refreshToken 쿠키가 없으면 인증 실패(401)
        HttpCookie refreshCookie = exchange.getRequest().getCookies().getFirst("refreshToken");
        if (refreshCookie == null) {
            log.warn("refreshToken 쿠키 없음 path={}", exchange.getRequest().getPath());
            return unauthorized(exchange);
        }
        String refreshToken = refreshCookie.getValue();

        if (!jwtUtil.validateToken(accessToken)) {
            log.warn("유효하지 않은 accessToken path={}", exchange.getRequest().getPath());
            return unauthorized(exchange);
        }

        if (!jwtUtil.validateToken(refreshToken)) {
            log.warn("유효하지 않은 refreshToken path={}", exchange.getRequest().getPath());
            return unauthorized(exchange);
        }

        if (blackListCheck.isBlacklisted(accessToken, "AT")) {
            log.warn("블랙리스트 accessToken 사용 시도 path={}", exchange.getRequest().getPath());
            return unauthorized(exchange);
        }

        // user-service 가 저장하는 키 형식과 맞춘다: "blacklist:RT:" + refreshToken
        if (blackListCheck.isBlacklisted(refreshToken, "RT")) {
            log.warn("블랙리스트 refreshToken 사용 시도 path={}", exchange.getRequest().getPath());
            return unauthorized(exchange);
        }

        Claims claims = jwtUtil.getClaims(accessToken);
        String userId = claims.getSubject();
        String username = claims.get("username", String.class);
        String role = claims.get("role", String.class);
        log.debug("인증 성공 userId={}, role={}", userId, role);

        UsernamePasswordAuthenticationToken authToken = new UsernamePasswordAuthenticationToken(
            username,
            null,
            Collections.singletonList(new SimpleGrantedAuthority(role))
        );
        authToken.setDetails(userId);
        SecurityContext securityContext = new SecurityContextImpl(authToken);

        ServerWebExchange mutatedExchange = exchange.mutate()
            .request(r -> r.headers(headers -> {
                headers.set("X-User-Id", userId);
                headers.set("X-User-Name", username);
                headers.set("X-User-Role", role);
                headers.set("X-ACCESS-TOKEN", accessToken);
                headers.set("X-REFRESH-TOKEN", refreshToken);
            }))
            .build();

        return chain.filter(mutatedExchange)
            .contextWrite(ReactiveSecurityContextHolder.withSecurityContext(Mono.just(securityContext)));
    }

    private static Mono<Void> unauthorized(ServerWebExchange exchange) {
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        return exchange.getResponse().setComplete();
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }

}
