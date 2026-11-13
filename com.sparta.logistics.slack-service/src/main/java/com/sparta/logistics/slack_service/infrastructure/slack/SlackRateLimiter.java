package com.sparta.logistics.slack_service.infrastructure.slack;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Slack chat.postMessage 호출 한도에 맞춘 채널별 발송 간격 제한.
 *
 * Slack 은 chat.postMessage 를 채널당 대략 초당 1건으로 제한하고, 넘으면 429 와 Retry-After 를 돌려준다.
 * 같은 채널로 보내는 요청 사이에 최소 간격을 두고, 429 를 받으면 Retry-After 동안 그 채널을 쉬게 한다.
 *
 * 인스턴스 안에서만 조율한다. slack-service 를 여러 대 띄우면 합계가 한도를 넘을 수 있는데,
 * 그 경우 받는 429 는 Retry-After 를 지켜 재시도하므로 유실되지 않는다.
 */
@Component
public class SlackRateLimiter {

    private final long intervalNanos;
    private final long maxWaitNanos;
    /** 채널별로 다음 요청을 보내도 되는 시각 (System.nanoTime 기준) */
    private final Map<String, Long> nextAllowed = new HashMap<>();

    public SlackRateLimiter(@Value("${slack.rate-limit.per-channel-interval-ms:1000}") long intervalMs,
                            @Value("${slack.rate-limit.max-wait-ms:20000}") long maxWaitMs) {
        this.intervalNanos = Duration.ofMillis(intervalMs).toNanos();
        this.maxWaitNanos = Duration.ofMillis(maxWaitMs).toNanos();
    }

    /**
     * 이 채널로 보낼 차례가 될 때까지 기다린다.
     * 기다릴 시간이 최대 대기 시간을 넘으면 기다리지 않고 재시도 예외를 던진다 (컨슈머 스레드를 오래 붙잡지 않도록).
     */
    public void acquire(String channel) {
        long waitNanos = reserve(channel);
        if (waitNanos < 0) {
            throw new SlackRetryableException("Slack 채널 발송 대기 시간이 너무 김 - 잠시 후 재시도");
        }
        if (waitNanos > 0) {
            try {
                TimeUnit.NANOSECONDS.sleep(waitNanos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SlackRetryableException("Slack 발송 대기 중 인터럽트", e);
            }
        }
    }

    /** 429 를 받았을 때 Retry-After 동안 이 채널로 보내지 않는다. */
    public synchronized void pause(String channel, Duration retryAfter) {
        long until = System.nanoTime() + retryAfter.toNanos();
        nextAllowed.merge(channel, until, Math::max);
    }

    /** @return 기다릴 시간(ns). 최대 대기 시간을 넘으면 -1 (예약하지 않음) */
    private synchronized long reserve(String channel) {
        long now = System.nanoTime();
        long start = Math.max(now, nextAllowed.getOrDefault(channel, now));
        long wait = start - now;
        if (wait > maxWaitNanos) {
            return -1;
        }
        nextAllowed.put(channel, start + intervalNanos);
        return wait;
    }
}
