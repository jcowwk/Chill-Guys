package com.sparta.logistics.gateway.security.filter;

import com.sparta.logistics.gateway.jwt.JwtUtil;
import com.sparta.logistics.gateway.security.service.BlackListCheck;
import io.jsonwebtoken.Claims;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpCookie;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class PreAuthWebFilterTest {

    private static final String AT = "access-token";
    private static final String RT = "refresh-token";

    private JwtUtil jwtUtil;
    private BlackListCheck blackListCheck;
    private PreAuthWebFilter filter;
    private final AtomicReference<ServerWebExchange> passed = new AtomicReference<>();
    private final WebFilterChain chain = exchange -> {
        passed.set(exchange);
        return Mono.empty();
    };

    @BeforeEach
    void setUp() {
        jwtUtil = mock(JwtUtil.class);
        blackListCheck = mock(BlackListCheck.class);
        filter = new PreAuthWebFilter(jwtUtil, blackListCheck);
        when(jwtUtil.removeBearer("Bearer " + AT)).thenReturn(AT);
        when(jwtUtil.validateToken(anyString())).thenReturn(true);
        Claims claims = mock(Claims.class);
        when(claims.getSubject()).thenReturn("1");
        when(claims.get("username", String.class)).thenReturn("master");
        when(claims.get("role", String.class)).thenReturn("ROLE_MASTER");
        when(jwtUtil.getClaims(AT)).thenReturn(claims);
    }

    private MockServerWebExchange exchange(boolean withCookie) {
        MockServerHttpRequest.BaseBuilder<?> req = MockServerHttpRequest.get("/api/orders")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + AT);
        if (withCookie) req.cookie(new HttpCookie("refreshToken", RT));
        return MockServerWebExchange.from(req);
    }

    @Test
    void 정상_토큰이면_사용자_헤더를_붙여_통과시킨다() {
        filter.filter(exchange(true), chain).block();

        assertThat(passed.get()).isNotNull();
        HttpHeaders headers = passed.get().getRequest().getHeaders();
        assertThat(headers.getFirst("X-User-Id")).isEqualTo("1");
        assertThat(headers.getFirst("X-User-Role")).isEqualTo("ROLE_MASTER");
    }

    @Test
    void refreshToken_쿠키가_없으면_401() {
        MockServerWebExchange exchange = exchange(false);

        filter.filter(exchange, chain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(passed.get()).isNull();
    }

    @Test
    void 블랙리스트_refreshToken_이면_401() {
        when(blackListCheck.isBlacklisted(RT, "RT")).thenReturn(true);
        MockServerWebExchange exchange = exchange(true);

        filter.filter(exchange, chain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(passed.get()).isNull();
        verify(blackListCheck, never()).isBlacklisted(AT, "RT");
    }

    @Test
    void 블랙리스트_accessToken_이면_401() {
        when(blackListCheck.isBlacklisted(AT, "AT")).thenReturn(true);
        MockServerWebExchange exchange = exchange(true);

        filter.filter(exchange, chain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }
}
