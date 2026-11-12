package com.sparta.logistics.delivery_service.domain.model;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Transactional Outbox.
 * 비즈니스 데이터와 같은 트랜잭션으로 저장되어, DB 커밋 = 이벤트 발행 보장이 된다.
 * 실제 Kafka 발행은 OutboxRelay 가 비동기로 수행한다.
 * id 는 Kafka 헤더(eventId)로 함께 전송되어 컨슈머 측 중복 처리 방지 키로 사용된다.
 */
@Entity
@Getter
@Table(name = "p_outbox_event",
        indexes = @Index(name = "idx_outbox_status_next_retry", columnList = "status, next_retry_at"))
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OutboxEvent {

    private static final int MAX_ERROR_LENGTH = 1000;

    @Id
    private UUID id;

    @Column(nullable = false)
    private UUID aggregateId;

    @Column(nullable = false)
    private String topic;

    @Column(name = "message_key", nullable = false)
    private String messageKey;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private OutboxStatus status;

    @Column(nullable = false)
    private int retryCount;

    @Column(name = "next_retry_at", nullable = false)
    private LocalDateTime nextRetryAt;

    @Column(length = MAX_ERROR_LENGTH)
    private String lastError;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    private LocalDateTime publishedAt;

    private OutboxEvent(UUID aggregateId, String topic, String messageKey, String payload, LocalDateTime now) {
        this.id = UUID.randomUUID();
        this.aggregateId = aggregateId;
        this.topic = topic;
        this.messageKey = messageKey;
        this.payload = payload;
        this.status = OutboxStatus.PENDING;
        this.retryCount = 0;
        this.nextRetryAt = now;
        this.createdAt = now;
    }

    public static OutboxEvent create(UUID aggregateId, String topic, String payload, LocalDateTime now) {
        // 같은 aggregate 의 이벤트는 같은 파티션으로 가도록 key 지정 (순서 보장)
        return new OutboxEvent(aggregateId, topic, aggregateId.toString(), payload, now);
    }

    public void markPublished(LocalDateTime now) {
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = now;
        this.lastError = null;
        // 발행이 끝난 이벤트는 다시 보낼 일이 없으므로 본문(주소 등 개인정보)을 남기지 않는다
        this.payload = "";
    }

    /**
     * 발행 실패 기록.
     * - 일시적 장애(브로커 다운, 타임아웃 등): 지수 백오프로 무기한 재시도 (Kafka 가 복구되면 자동 발행)
     * - 영구적 오류(직렬화 실패, 메시지 크기 초과 등): maxRetries 초과 시 FAILED 로 격리
     */
    public void recordFailure(String error, boolean retriable, int maxRetries,
                              Duration baseBackoff, Duration maxBackoff, LocalDateTime now) {
        this.retryCount++;
        this.lastError = truncate(error);

        if (!retriable && this.retryCount >= maxRetries) {
            this.status = OutboxStatus.FAILED;
            return;
        }
        this.nextRetryAt = now.plus(backoff(this.retryCount, baseBackoff, maxBackoff));
    }

    static Duration backoff(int retryCount, Duration base, Duration max) {
        int exponent = Math.min(Math.max(retryCount - 1, 0), 20); // overflow 방지
        Duration candidate = base.multipliedBy(1L << exponent);
        return candidate.compareTo(max) > 0 ? max : candidate;
    }

    private static String truncate(String error) {
        if (error == null) return null;
        return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
    }
}
