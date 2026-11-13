package com.sparta.logistics.slack_service.infrastructure.monitoring;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Kafka → Slack 알림 구간의 운영 지표 (Prometheus: /actuator/prometheus, 관리 포트).
 * 지표 이름/태그에 주문 ID, 주소, Slack ID 같은 개인정보를 넣지 않는다.
 */
@Component
public class NotificationMetrics {

    private final MeterRegistry registry;
    private final Counter processed;
    private final Counter duplicate;
    private final Counter retry;
    private final Counter aiFallback;
    private final Counter slackSendFailure;

    public NotificationMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.processed = Counter.builder("slack.notification.processed").description("처리 완료한 알림").register(registry);
        this.duplicate = Counter.builder("slack.notification.duplicate").description("이미 처리해서 건너뛴 알림 (중복 수신)").register(registry);
        this.retry = Counter.builder("slack.notification.retry").description("처리 실패 후 재시도").register(registry);
        this.aiFallback = Counter.builder("slack.ai.fallback").description("AI 실패로 대체 문구 발송").register(registry);
        this.slackSendFailure = Counter.builder("slack.send.failures").description("Slack API 발송 실패 (재시도 없음)").register(registry);
    }

    public void processed() { processed.increment(); }

    public void duplicate() { duplicate.increment(); }

    public void retry() { retry.increment(); }

    public void aiFallback() { aiFallback.increment(); }

    public void slackSendFailure() { slackSendFailure.increment(); }

    /** 재시도 끝에 DLT 로 보낸 알림. reason 은 예외 클래스 이름(개인정보 아님) */
    public void sentToDlt(String topic, String reason) {
        Counter.builder("slack.notification.dlt").description("재시도 실패로 DLT 에 격리된 알림")
                .tag("topic", topic).tag("reason", reason).register(registry).increment();
    }
}
