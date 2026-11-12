package com.sparta.logistics.delivery_service.domain.model;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxEventTest {

    private static final Duration BASE = Duration.ofSeconds(5);
    private static final Duration MAX = Duration.ofMinutes(5);
    private final LocalDateTime now = LocalDateTime.of(2026, 1, 1, 0, 0);

    @Test
    void 생성시_PENDING_이고_aggregateId_를_key_로_사용한다() {
        UUID deliveryId = UUID.randomUUID();
        OutboxEvent event = OutboxEvent.create(deliveryId, "HUB", "{}", now);

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getMessageKey()).isEqualTo(deliveryId.toString());
        assertThat(event.getNextRetryAt()).isEqualTo(now);
        assertThat(event.getId()).isNotNull();
    }

    @Test
    void 일시적_장애는_재시도_한도를_넘어도_FAILED_가_되지_않는다() {
        OutboxEvent event = OutboxEvent.create(UUID.randomUUID(), "HUB", "{}", now);

        for (int i = 0; i < 50; i++) {
            event.recordFailure("broker down", true, 3, BASE, MAX, now);
        }

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getNextRetryAt()).isEqualTo(now.plus(MAX));
    }

    @Test
    void 영구_오류는_재시도_한도_도달시_FAILED() {
        OutboxEvent event = OutboxEvent.create(UUID.randomUUID(), "HUB", "{}", now);

        event.recordFailure("too large", false, 2, BASE, MAX, now);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);

        event.recordFailure("too large", false, 2, BASE, MAX, now);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.FAILED);
    }

    @Test
    void 백오프는_지수적으로_증가하고_최대값으로_제한된다() {
        assertThat(OutboxEvent.backoff(1, BASE, MAX)).isEqualTo(Duration.ofSeconds(5));
        assertThat(OutboxEvent.backoff(2, BASE, MAX)).isEqualTo(Duration.ofSeconds(10));
        assertThat(OutboxEvent.backoff(3, BASE, MAX)).isEqualTo(Duration.ofSeconds(20));
        assertThat(OutboxEvent.backoff(100, BASE, MAX)).isEqualTo(MAX);
    }

    @Test
    void 발행_성공시_PUBLISHED() {
        OutboxEvent event = OutboxEvent.create(UUID.randomUUID(), "HUB", "{}", now);
        event.recordFailure("broker down", true, 3, BASE, MAX, now);

        event.markPublished(now);

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(event.getPublishedAt()).isEqualTo(now);
        assertThat(event.getLastError()).isNull();
    }
}
