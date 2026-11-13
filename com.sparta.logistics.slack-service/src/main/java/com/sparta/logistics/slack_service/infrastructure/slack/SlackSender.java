package com.sparta.logistics.slack_service.infrastructure.slack;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sparta.logistics.slack_service.application.dto.request.SlackMessageSendRequestDto;
import com.sparta.logistics.slack_service.infrastructure.client.SlackClient;
import com.sparta.logistics.slack_service.infrastructure.config.ResilienceConfig;
import feign.FeignException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.circuitbreaker.CircuitBreakerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Slack 메시지 발송과 실패 판정.
 *
 * - 발송 전: 채널별 간격 제한 (SlackRateLimiter)
 * - 429: Retry-After 만큼 채널을 쉬게 하고 재시도 예외
 * - 5xx, 네트워크, 서킷 열림, 일시 오류(ok:false + internal_error 등): 재시도 예외
 * - 그 외 ok:false (channel_not_found, invalid_auth, not_in_channel ...) / 4xx: 영구 실패 예외 → 바로 DLT
 *
 * 실패를 삼키지 않고 예외로 알리는 것이 핵심이다. 삼키면 "처리 완료"로 기록돼 알림이 조용히 사라진다.
 */
@Slf4j
@Component
public class SlackSender {

    /** Slack 문서상 재시도하면 성공할 수 있는 오류 코드 */
    private static final Set<String> TRANSIENT_ERRORS =
            Set.of("ratelimited", "internal_error", "fatal_error", "service_unavailable", "request_timeout");
    private static final Duration DEFAULT_RETRY_AFTER = Duration.ofSeconds(1);

    private final SlackClient slackClient;
    private final CircuitBreakerFactory<?, ?> circuitBreakerFactory;
    private final SlackRateLimiter rateLimiter;
    private final String botToken;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public SlackSender(SlackClient slackClient, CircuitBreakerFactory<?, ?> circuitBreakerFactory,
                       SlackRateLimiter rateLimiter, @Value("${slack.bot.token}") String botToken) {
        this.slackClient = slackClient;
        this.circuitBreakerFactory = circuitBreakerFactory;
        this.rateLimiter = rateLimiter;
        this.botToken = botToken;
    }

    /** 성공하면 정상 반환, 실패하면 SlackRetryableException 또는 SlackPermanentException */
    public void send(UUID messageId, String channel, String text) {
        rateLimiter.acquire(channel);

        SlackMessageSendRequestDto request = SlackMessageSendRequestDto.builder()
                .id(messageId).channel(channel).text(text).build();
        String body;
        try {
            body = circuitBreakerFactory.create(ResilienceConfig.SLACK).run(
                    () -> slackClient.sendSlackMessage("Bearer " + botToken, request),
                    throwable -> { throw asRuntime(throwable); });
        } catch (FeignException e) {
            throw classifyHttpFailure(e, channel);
        } catch (CallNotPermittedException e) {
            throw new SlackRetryableException("Slack 서킷 열림 - 잠시 후 재시도", e);
        } catch (SlackRetryableException | SlackPermanentException e) {
            throw e;
        } catch (RuntimeException e) {
            // 시간 초과 등 원인을 특정할 수 없는 실패는 다시 시도해 본다
            throw new SlackRetryableException("Slack 호출 실패: " + e.getClass().getSimpleName(), e);
        }
        checkResponse(body, channel);
    }

    private RuntimeException classifyHttpFailure(FeignException e, String channel) {
        int status = e.status();
        if (status == 429) {
            Duration retryAfter = retryAfter(e.responseHeaders());
            rateLimiter.pause(channel, retryAfter);
            return new SlackRetryableException("Slack 호출 한도 초과(429), " + retryAfter.toSeconds() + "초 후 재시도", e);
        }
        if (status < 0 || status >= 500 || status == 408) {
            return new SlackRetryableException("Slack 일시 오류(HTTP " + status + ")", e);
        }
        return new SlackPermanentException("Slack 요청 거부(HTTP " + status + ")");
    }

    private void checkResponse(String body, String channel) {
        JsonNode json;
        try {
            json = objectMapper.readTree(body);
        } catch (Exception e) {
            throw new SlackRetryableException("Slack 응답 해석 실패", e);
        }
        if (json != null && json.path("ok").asBoolean(false)) {
            return;
        }
        String error = json == null ? "empty_response" : json.path("error").asText("unknown_error");
        if ("ratelimited".equals(error)) {
            rateLimiter.pause(channel, DEFAULT_RETRY_AFTER);
        }
        if (TRANSIENT_ERRORS.contains(error)) {
            throw new SlackRetryableException("Slack 일시 오류: " + error);
        }
        throw new SlackPermanentException("Slack 발송 거부: " + error);
    }

    static Duration retryAfter(Map<String, Collection<String>> headers) {
        if (headers == null) return DEFAULT_RETRY_AFTER;
        for (Map.Entry<String, Collection<String>> h : headers.entrySet()) {
            if ("retry-after".equalsIgnoreCase(h.getKey()) && h.getValue() != null) {
                for (String v : h.getValue()) {
                    try {
                        return Duration.ofSeconds(Math.max(1, Long.parseLong(v.trim())));
                    } catch (NumberFormatException ignored) {
                        // 형식이 다르면 기본값
                    }
                }
            }
        }
        return DEFAULT_RETRY_AFTER;
    }

    private static RuntimeException asRuntime(Throwable t) {
        return t instanceof RuntimeException r ? r : new SlackRetryableException("Slack 호출 실패", t);
    }
}
