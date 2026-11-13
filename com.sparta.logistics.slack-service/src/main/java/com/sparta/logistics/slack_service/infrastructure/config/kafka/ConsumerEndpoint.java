package com.sparta.logistics.slack_service.infrastructure.config.kafka;

import com.sparta.logistics.slack_service.application.service.SlackService;
import com.sparta.logistics.slack_service.domain.entity.ProcessedEvent;
import com.sparta.logistics.slack_service.domain.repository.ProcessedEventRepository;
import com.sparta.logistics.slack_service.infrastructure.dto.DeliveryInfoDto;
import com.sparta.logistics.slack_service.infrastructure.monitoring.NotificationMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumerEndpoint {
    private final SlackService slackService;
    private final ProcessedEventRepository processedEventRepository;
    private final NotificationMetrics metrics;

    @KafkaListener(groupId = "hub", topics = KafkaTopics.HUB)
    public void hubListen(ConsumerRecord<String, DeliveryInfoDto> record) {
        log.debug("HUB 메시지 수신 partition={}, offset={}", record.partition(), record.offset());
        handle(record);
    }

    @KafkaListener(groupId = "company", topics = KafkaTopics.COMPANY)
    public void listen(ConsumerRecord<String, DeliveryInfoDto> record) {
        log.debug("COMPANY 메시지 수신 partition={}, offset={}", record.partition(), record.offset());
        handle(record);
    }

    /**
     * 멱등 처리: 이미 처리한 eventId 는 건너뛴다 (Producer 재발행/컨슈머 재처리 시 Slack 중복 발송 방지).
     * 처리 중 예외는 그대로 던져 에러 핸들러가 재시도 → DLT 로 보내도록 한다.
     */
    void handle(ConsumerRecord<String, DeliveryInfoDto> record) {
        String eventId = extractEventId(record);

        if (eventId != null && processedEventRepository.existsById(eventId)) {
            log.info("이미 처리된 이벤트 skip eventId={}, topic={}", eventId, record.topic());
            metrics.duplicate();
            return;
        }

        slackService.saveSlackMessage(record.value());
        metrics.processed();

        if (eventId != null) {
            markProcessed(eventId, record.topic());
        }
    }

    private void markProcessed(String eventId, String topic) {
        try {
            processedEventRepository.save(new ProcessedEvent(eventId, topic, LocalDateTime.now()));
        } catch (Exception e) {
            // 메시지는 이미 처리되었으므로 여기서 예외를 던지면 재시도로 오히려 중복 발송된다 → 로그만 남김
            log.error("처리 완료 기록 실패 eventId={}: {}", eventId, e.toString());
        }
    }

    private static String extractEventId(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(KafkaTopics.EVENT_ID_HEADER);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
