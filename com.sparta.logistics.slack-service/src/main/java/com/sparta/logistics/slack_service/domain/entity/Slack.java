package com.sparta.logistics.slack_service.domain.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.*;

import java.sql.Timestamp;
import java.util.UUID;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "p_slack",
        indexes = @Index(name = "uk_slack_event_id", columnList = "event_id", unique = true))
public class Slack {
    @Id
    private UUID id;
    private String slackId;
    // 알림 문구(주소, 허브명, 주문/상품 ID, AI 응답)는 255자를 쉽게 넘는다.
    // 기본 매핑(varchar(255))이면 저장이 실패해 알림 전체가 DLT 로 간다. (전체 서비스 부하 테스트에서 발견)
    @Column(columnDefinition = "TEXT")
    private String text;
    private Timestamp createdAt;
    private Long createdBy;

    /** Kafka 이벤트 ID. 재시도/재처리 때 이미 만든 알림을 다시 만들지 않고 재사용하기 위함 */
    @Column(name = "event_id", length = 64)
    private String eventId;

    /** Slack 발송 완료 시각. 값이 있으면 다시 보내지 않는다 */
    private Timestamp sentAt;

    public void markSent(Timestamp at) {
        this.sentAt = at;
    }
}
