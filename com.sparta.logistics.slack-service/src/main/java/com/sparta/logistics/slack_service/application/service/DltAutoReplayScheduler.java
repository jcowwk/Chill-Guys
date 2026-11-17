package com.sparta.logistics.slack_service.application.service;

import com.sparta.logistics.slack_service.infrastructure.config.kafka.KafkaTopics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * DLT 에 들어간 메시지를 주기적으로 원본 토픽에 다시 넣는다.
 *
 * 컨슈머 재시도(약 3분)보다 오래 멈춘 내부 서비스가 돌아오면 사람이 재처리하지 않아도 알림이 나간다.
 * 원인이 그대로면 다시 DLT 로 오고, 재처리 횟수 한도(기본 3회)에서 멈춘다.
 * 이미 처리된 메시지는 eventId 로 걸러지므로 여러 번 재투입돼도 알림은 한 번만 나간다.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "kafka.dlt.auto-replay.enabled", havingValue = "true", matchIfMissing = true)
public class DltAutoReplayScheduler {

    private static final List<String> TOPICS = List.of(KafkaTopics.HUB, KafkaTopics.COMPANY);

    private final DltReplayService dltReplayService;
    private final int batchSize;

    public DltAutoReplayScheduler(DltReplayService dltReplayService,
                                  @Value("${kafka.dlt.auto-replay.batch-size:500}") int batchSize) {
        this.dltReplayService = dltReplayService;
        this.batchSize = batchSize;
    }

    @Scheduled(initialDelayString = "${kafka.dlt.auto-replay.interval-ms:600000}",
               fixedDelayString = "${kafka.dlt.auto-replay.interval-ms:600000}")
    public void replay() {
        for (String topic : TOPICS) {
            try {
                dltReplayService.autoReplay(topic, batchSize);
            } catch (Exception e) {
                // Kafka 장애, 수동 재처리와 겹침 등: 다음 주기에 다시 시도
                log.warn("[DLT] 자동 재처리 실패 topic={}: {}", topic, e.toString());
            }
        }
    }
}
