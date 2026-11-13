package com.sparta.logistics.slack_service.infrastructure.slack;

import com.sparta.logistics.slack_service.infrastructure.client.SlackClient;
import com.sparta.logistics.slack_service.infrastructure.config.ResilienceConfig;
import feign.FeignException;
import feign.Request;
import feign.Response;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JCircuitBreakerFactory;
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JConfigurationProperties;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SlackSenderTest {

    private SlackClient slackClient;
    private SlackRateLimiter limiter;
    private SlackSender sender;

    @BeforeEach
    void setUp() {
        slackClient = mock(SlackClient.class);
        limiter = new SlackRateLimiter(0, 500);
        Resilience4JCircuitBreakerFactory factory = new Resilience4JCircuitBreakerFactory(
                CircuitBreakerRegistry.ofDefaults(), TimeLimiterRegistry.ofDefaults(), null, new Resilience4JConfigurationProperties());
        new ResilienceConfig().externalApiCircuitBreakerCustomizer().customize(factory);
        sender = new SlackSender(slackClient, factory, limiter, "test-token");
    }

    private void send() {
        sender.send(UUID.randomUUID(), "C1", "배송 알림");
    }

    @Test
    void 성공_응답이면_예외_없음() {
        when(slackClient.sendSlackMessage(anyString(), any())).thenReturn("{\"ok\":true}");
        assertThatCode(this::send).doesNotThrowAnyException();
    }

    @Test
    void 호출_한도_429_면_Retry_After_동안_채널을_쉬고_재시도_예외() {
        when(slackClient.sendSlackMessage(anyString(), any())).thenThrow(httpError(429, Map.of("Retry-After", List.of("3"))));

        assertThatThrownBy(this::send).isInstanceOf(SlackRetryableException.class).hasMessageContaining("3초");
        // 3초 쉬어야 하므로 최대 대기(0.5초)를 넘어 바로 재시도로 넘어감
        assertThatThrownBy(() -> limiter.acquire("C1")).isInstanceOf(SlackRetryableException.class);
    }

    @Test
    void ok_false_ratelimited_와_일시_오류는_재시도() {
        when(slackClient.sendSlackMessage(anyString(), any()))
                .thenReturn("{\"ok\":false,\"error\":\"internal_error\"}");
        assertThatThrownBy(this::send).isInstanceOf(SlackRetryableException.class).hasMessageContaining("internal_error");
    }

    @Test
    void 채널_없음_같은_설정_오류는_재시도하지_않는_영구_실패() {
        when(slackClient.sendSlackMessage(anyString(), any()))
                .thenReturn("{\"ok\":false,\"error\":\"channel_not_found\"}");
        assertThatThrownBy(this::send).isInstanceOf(SlackPermanentException.class).hasMessageContaining("channel_not_found");
    }

    @Test
    void HTTP_5xx_는_재시도_4xx_는_영구_실패() {
        when(slackClient.sendSlackMessage(anyString(), any())).thenThrow(httpError(503, Map.of()));
        assertThatThrownBy(this::send).isInstanceOf(SlackRetryableException.class);

        doThrow(httpError(403, Map.of())).when(slackClient).sendSlackMessage(anyString(), any());
        assertThatThrownBy(this::send).isInstanceOf(SlackPermanentException.class);
    }

    @Test
    void 네트워크_오류는_재시도() {
        when(slackClient.sendSlackMessage(anyString(), any())).thenThrow(new RuntimeException("connect timed out"));
        assertThatThrownBy(this::send).isInstanceOf(SlackRetryableException.class);
    }

    private static FeignException httpError(int status, Map<String, Collection<String>> headers) {
        Request request = Request.create(Request.HttpMethod.POST, "https://slack.com/api/chat.postMessage",
                Map.of(), null, StandardCharsets.UTF_8, null);
        Response response = Response.builder().status(status).reason("error").headers(headers)
                .request(request).body(new byte[0]).build();
        return FeignException.errorStatus("SlackClient#sendSlackMessage", response);
    }
}
