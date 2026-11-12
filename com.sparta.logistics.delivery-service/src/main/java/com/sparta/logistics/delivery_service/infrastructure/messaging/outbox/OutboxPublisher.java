package com.sparta.logistics.delivery_service.infrastructure.messaging.outbox;

import com.sparta.logistics.delivery_service.domain.model.OutboxEvent;
import com.sparta.logistics.delivery_service.domain.model.OutboxStatus;
import com.sparta.logistics.delivery_service.domain.repository.OutboxEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Outbox 에 쌓인 이벤트를 Kafka 로 발행한다.
 * 브로커 ack 를 동기로 확인한 뒤에만 PUBLISHED 로 바꾸므로 Kafka 장애 시에도 이벤트가 유실되지 않는다.
 * (발행 성공 후 DB 갱신 전에 장애가 나면 재발행될 수 있음 → at-least-once, 컨슈머가 eventId 로 중복 제거)
 */
@Slf4j
@Component
public class OutboxPublisher {

    public static final String EVENT_ID_HEADER = "eventId";

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> outboxKafkaTemplate;

    private final int batchSize;
    private final long sendTimeoutMs;
    private final int maxRetries;
    private final Duration baseBackoff;
    private final Duration maxBackoff;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository,
                           KafkaTemplate<String, String> outboxKafkaTemplate,
                           @Value("${outbox.relay.batch-size:100}") int batchSize,
                           @Value("${outbox.relay.send-timeout-ms:20000}") long sendTimeoutMs,
                           @Value("${outbox.relay.max-retries:10}") int maxRetries,
                           @Value("${outbox.relay.base-backoff-ms:5000}") long baseBackoffMs,
                           @Value("${outbox.relay.max-backoff-ms:300000}") long maxBackoffMs) {
        this.outboxEventRepository = outboxEventRepository;
        this.outboxKafkaTemplate = outboxKafkaTemplate;
        this.batchSize = batchSize;
        this.sendTimeoutMs = sendTimeoutMs;
        this.maxRetries = maxRetries;
        this.baseBackoff = Duration.ofMillis(baseBackoffMs);
        this.maxBackoff = Duration.ofMillis(maxBackoffMs);
    }

    /**
     * @return 이번 주기에 발행 성공한 이벤트 수
     */
    @Transactional
    public int publishPendingEvents() {
        List<OutboxEvent> events = outboxEventRepository.findPublishableForUpdate(LocalDateTime.now(), batchSize);
        int published = 0;

        for (OutboxEvent event : events) {
            try {
                outboxKafkaTemplate.send(toRecord(event)).get(sendTimeoutMs, TimeUnit.MILLISECONDS);
                event.markPublished(LocalDateTime.now());
                published++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                handleFailure(event, e);
                break;
            } catch (Exception e) {
                handleFailure(event, e);
                // 브로커 장애일 가능성이 높으므로 남은 이벤트는 다음 주기로 미룬다 (장애 중 불필요한 대기/부하 방지)
                break;
            }
        }
        return published;
    }

    @Transactional
    public int purgePublishedBefore(LocalDateTime before) {
        return outboxEventRepository.deleteByStatusAndPublishedAtBefore(OutboxStatus.PUBLISHED, before);
    }

    private ProducerRecord<String, String> toRecord(OutboxEvent event) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(event.getTopic(), event.getMessageKey(), event.getPayload());
        record.headers().add(EVENT_ID_HEADER, event.getId().toString().getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private void handleFailure(OutboxEvent event, Exception e) {
        Throwable cause = rootCause(e);
        boolean retriable = isRetriable(e);
        event.recordFailure(cause.toString(), retriable, maxRetries, baseBackoff, maxBackoff, LocalDateTime.now());

        if (event.getStatus() == OutboxStatus.FAILED) {
            log.error("[Outbox] 이벤트 발행 영구 실패 - 수동 확인 필요 eventId={}, topic={}, aggregateId={}, error={}",
                    event.getId(), event.getTopic(), event.getAggregateId(), cause.toString());
        } else {
            log.warn("[Outbox] 이벤트 발행 실패, 재시도 예정 eventId={}, topic={}, retryCount={}, nextRetryAt={}, error={}",
                    event.getId(), event.getTopic(), event.getRetryCount(), event.getNextRetryAt(), cause.toString());
        }
    }

    /**
     * 브로커 다운/네트워크/타임아웃 계열은 Kafka 가 복구되면 성공하므로 재시도 횟수 제한 없이 재시도한다.
     */
    static boolean isRetriable(Throwable e) {
        Throwable current = e;
        while (current != null) {
            if (current instanceof RetriableException
                    || current instanceof TimeoutException
                    || current instanceof InterruptedException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static Throwable rootCause(Throwable e) {
        Throwable current = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}
