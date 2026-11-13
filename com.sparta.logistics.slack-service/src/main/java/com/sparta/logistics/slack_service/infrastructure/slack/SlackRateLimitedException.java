package com.sparta.logistics.slack_service.infrastructure.slack;

import java.time.Duration;

/**
 * Slack 이 호출 한도 초과(429)로 거절한 경우.
 * 장애가 아니라 "천천히 보내라"는 신호이므로 서킷 브레이커 실패로 세지 않는다 (ResilienceConfig 에서 제외).
 */
public class SlackRateLimitedException extends SlackRetryableException {

    private final Duration retryAfter;

    public SlackRateLimitedException(Duration retryAfter) {
        super("Slack 호출 한도 초과(429), " + retryAfter.toSeconds() + "초 후 재시도");
        this.retryAfter = retryAfter;
    }

    public Duration getRetryAfter() {
        return retryAfter;
    }
}
