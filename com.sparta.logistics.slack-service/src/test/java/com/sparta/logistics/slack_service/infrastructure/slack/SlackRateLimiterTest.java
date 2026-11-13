package com.sparta.logistics.slack_service.infrastructure.slack;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SlackRateLimiterTest {

    @Test
    void 같은_채널은_최소_간격을_두고_보낸다() {
        SlackRateLimiter limiter = new SlackRateLimiter(200, 5_000);

        long start = System.nanoTime();
        for (int i = 0; i < 3; i++) limiter.acquire("C1");
        long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();

        assertThat(elapsedMs).isBetween(380L, 1_000L); // 0, 200, 400ms
    }

    @Test
    void 다른_채널끼리는_기다리지_않는다() {
        SlackRateLimiter limiter = new SlackRateLimiter(1_000, 5_000);

        long start = System.nanoTime();
        for (int i = 0; i < 5; i++) limiter.acquire("C" + i);

        assertThat(Duration.ofNanos(System.nanoTime() - start).toMillis()).isLessThan(100);
    }

    @Test
    void Retry_After_동안_채널을_쉬게_하고_최대_대기를_넘으면_재시도로_넘긴다() {
        SlackRateLimiter limiter = new SlackRateLimiter(0, 500);
        limiter.pause("C1", Duration.ofSeconds(3));

        assertThatThrownBy(() -> limiter.acquire("C1")).isInstanceOf(SlackRetryableException.class);
        // 다른 채널은 영향 없음
        limiter.acquire("C2");
    }
}
