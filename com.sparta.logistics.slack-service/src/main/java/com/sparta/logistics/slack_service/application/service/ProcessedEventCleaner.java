package com.sparta.logistics.slack_service.application.service;

import com.sparta.logistics.slack_service.domain.repository.ProcessedEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * 멱등 처리용 처리 이력(p_processed_event)을 주기적으로 정리한다.
 *
 * 보관 기간은 "같은 메시지가 다시 들어올 수 있는 최대 기간"보다 길어야 한다.
 * 가장 늦은 경우는 Kafka 토픽 보관 기간(기본 7일) 안의 메시지를 오프셋 리셋 등으로 다시 읽는 경우이므로
 * 토픽 retention 이상으로 둔다. 더 짧으면 다시 읽은 메시지가 중복으로 걸러지지 않아 Slack 이 또 발송된다.
 */
@Slf4j
@Component
public class ProcessedEventCleaner {

    private final ProcessedEventRepository processedEventRepository;
    private final int retentionDays;

    public ProcessedEventCleaner(ProcessedEventRepository processedEventRepository,
                                 @Value("${kafka.consumer.processed-event.retention-days:7}") int retentionDays) {
        this.processedEventRepository = processedEventRepository;
        this.retentionDays = retentionDays;
    }

    @Scheduled(cron = "${kafka.consumer.processed-event.purge-cron:0 30 4 * * *}")
    public void scheduledPurge() {
        try {
            purge(LocalDateTime.now());
        } catch (Exception e) {
            log.error("[ProcessedEvent] 정리 실패: {}", e.toString());
        }
    }

    @Transactional
    public int purge(LocalDateTime now) {
        int deleted = processedEventRepository.deleteByProcessedAtBefore(now.minusDays(retentionDays));
        log.info("[ProcessedEvent] {}일 지난 처리 이력 {}건 정리", retentionDays, deleted);
        return deleted;
    }
}
