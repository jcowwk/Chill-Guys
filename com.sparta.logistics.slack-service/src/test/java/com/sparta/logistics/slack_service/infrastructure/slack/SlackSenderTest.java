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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SlackSenderTest {

    private SlackClient slackClient;
    private SlackRateLimiter limiter;
    private SlackSender sender;
    private Resilience4JCircuitBreakerFactory factory;

    @BeforeEach
    void setUp() {
        slackClient = mock(SlackClient.class);
        limiter = new SlackRateLimiter(0, 500);
        factory = new Resilience4JCircuitBreakerFactory(
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
    void 호출_한도_429_면_Retry_After_만큼_기다렸다가_같은_메시지를_다시_보낸다() {
        sender = new SlackSender(slackClient, factory, new SlackRateLimiter(0, 5_000), "test-token");
        when(slackClient.sendSlackMessage(anyString(), any()))
                .thenThrow(httpError(429, Map.of("Retry-After", List.of("1"))))
                .thenReturn("{\"ok\":true}");

        long start = System.nanoTime();
        assertThatCode(this::send).doesNotThrowAnyException();
        long elapsedMs = java.time.Duration.ofNanos(System.nanoTime() - start).toMillis();

        assertThat(elapsedMs).isGreaterThanOrEqualTo(900); // Retry-After 1초 준수
        verify(slackClient, times(2)).sendSlackMessage(anyString(), any());
    }

    @Test
    void 호출_한도_429_가_최대_대기_시간을_넘게_계속되면_재시도_예외로_넘긴다() {
        sender = new SlackSender(slackClient, factory, new SlackRateLimiter(0, 5_000), "test-token",
                java.time.Duration.ofMillis(1_500));
        when(slackClient.sendSlackMessage(anyString(), any()))
                .thenThrow(httpError(429, Map.of("Retry-After", List.of("1"))));

        assertThatThrownBy(this::send).isInstanceOf(SlackRateLimitedException.class);
    }

    @Test
    void 호출_한도_429_는_서킷_브레이커_실패로_세지_않는다() {
        sender = new SlackSender(slackClient, factory, new SlackRateLimiter(0, 5_000), "test-token",
                java.time.Duration.ZERO);
        when(slackClient.sendSlackMessage(anyString(), any()))
                .thenThrow(httpError(429, Map.of("Retry-After", List.of("1"))));
        for (int i = 0; i < 20; i++) {
            assertThatThrownBy(this::send).isInstanceOf(SlackRateLimitedException.class);
        }

        // 429 가 20번 왔어도 서킷은 닫혀 있어 바로 정상 발송된다 (열렸다면 CallNotPermitted)
        doReturn("{\"ok\":true}").when(slackClient).sendSlackMessage(anyString(), any());
        assertThatCode(this::send).doesNotThrowAnyException();
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
