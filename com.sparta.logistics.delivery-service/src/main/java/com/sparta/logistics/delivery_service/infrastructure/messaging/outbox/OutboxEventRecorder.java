package com.sparta.logistics.delivery_service.infrastructure.messaging.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sparta.logistics.delivery_service.domain.model.OutboxEvent;
import com.sparta.logistics.delivery_service.domain.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Kafka 로 직접 보내지 않고 Outbox 테이블에 이벤트를 기록한다.
 * 반드시 비즈니스 트랜잭션 안에서 호출되어야 하므로 MANDATORY 로 강제한다.
 */
@Component
@RequiredArgsConstructor
public class OutboxEventRecorder {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional(propagation = Propagation.MANDATORY)
    public OutboxEvent record(UUID aggregateId, String topic, Object payload) {
        OutboxEvent event = OutboxEvent.create(aggregateId, topic, toJson(payload), LocalDateTime.now());
        return outboxEventRepository.save(event);
    }

    private String toJson(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            // 직렬화 실패는 비즈니스 트랜잭션까지 롤백시켜 상태 변경과 이벤트가 어긋나지 않게 한다
            throw new IllegalStateException("Outbox payload 직렬화 실패: " + payload.getClass().getSimpleName(), e);
        }
    }
}
