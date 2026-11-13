# 배송 알림 파이프라인 모니터링

delivery-service(배정 → Outbox → Kafka 발행)와 slack-service(Kafka 수신 → 알림)의 상태를
Prometheus 로 수집하고 Grafana 대시보드와 알람으로 확인한다.

```
delivery-service :29094/actuator/prometheus ─┐
slack-service    :29099/actuator/prometheus ─┼─▶ Prometheus :9090 (수집, 알람 규칙) ─▶ Grafana :3000 (대시보드)
```

## 실행

```bash
# 서비스(delivery, slack)가 떠 있는 상태에서
cd monitoring
GRAFANA_ADMIN_PASSWORD=<임의의 값> docker compose up -d
```

- Grafana: http://localhost:3000 — 로그인 없이 보기 전용으로 열린다. 편집은 `admin` / 위에서 넣은 비밀번호
- Prometheus: http://localhost:9090/alerts — 알람 상태

정리: `docker compose down`

## 지표

| 지표 | 의미 |
|---|---|
| `delivery_outbox_pending` | Kafka 로 아직 못 보낸 메시지 수 |
| `delivery_outbox_oldest_pending_age_seconds` | 가장 오래 기다린 미발행 메시지의 대기 시간 |
| `delivery_outbox_failed` | 영구 실패로 격리된 메시지 수 |
| `delivery_outbox_published_total` | Kafka 발행 성공 누적 |
| `delivery_outbox_publish_failures_total{type}` | 발행 실패 누적 (retriable / permanent) |
| `delivery_assignment_pending` | 담당자 배정 대기 배송 수 |
| `delivery_assignment_result_total{result}` | 배정 결과 누적 (assigned / failed) |
| `kafka_consumer_fetch_manager_records_lag` | 알림 컨슈머가 처리 못 하고 밀린 메시지 수 (파티션별, NaN 제외해서 합산) |
| `slack_notification_processed_total` | 알림 처리 누적 |
| `slack_notification_duplicate_total` | 중복 수신으로 건너뛴 알림 |
| `slack_notification_retry_total` | 처리 실패 후 재시도 |
| `slack_notification_dlt_total{topic,reason}` | 재시도 끝에 DLT 로 격리된 알림 |
| `slack_ai_fallback_total` | AI 실패로 대체 문구 발송 |
| `slack_send_failures_total{type}` | Slack 발송 실패 (retryable: 재시도 / permanent: 설정 오류로 바로 DLT) |
| `slack_ratelimited_total` | Slack 호출 한도 초과(429) 응답 |
| `resilience4j_circuitbreaker_state{name}` | 서킷 브레이커 상태 (ai, slack) |
| `spring_kafka_listener_seconds` | 알림 1건 처리 시간 |

## 알람 (prometheus/alerts.yml)

| 알람 | 조건 | 대응 |
|---|---|---|
| ServiceDown | 지표 수집 실패 30초 | 서비스 상태 확인 |
| OutboxPublishStuck | 미발행 메시지가 60초 넘게 대기 | Kafka 브로커 확인. 메시지는 보존되고 복구 시 자동 발행 |
| OutboxPermanentFailure | 영구 실패 1건 이상 | `p_outbox_event` 의 `last_error` 확인 후 재투입 |
| AssignmentBacklog | 배정 대기 3,000건 넘게 2분 | delivery-manager-service 또는 유입량 확인 |
| NotificationConsumerLag | 컨슈머 지연 1,000건 넘게 1분 | 외부 API 지연 또는 처리량 부족 |
| NotificationSentToDlt | 최근 5분 DLT 격리 1건 이상 | 원인 해결 후 `POST /api/master/slack/dlt/replay` |
| AiCircuitOpen | AI 서킷 열림 | Gemini 상태 확인. 알림은 대체 문구로 발송 중 |
| SlackPermanentFailure | 최근 5분 Slack 발송 거부(설정 오류) 1건 이상 | 채널, 토큰, 권한 확인 후 DLT 재처리 |
| SlackRateLimited | 최근 5분 429 응답 20회 초과 1분 | 알림 지연 중. 인스턴스 수와 채널별 발송량 확인 |

알람은 Prometheus 화면에서 확인한다. 메신저나 메일로 받으려면 Alertmanager 를 추가로 연결한다.

## 보안
- 지표는 서비스 포트가 아닌 관리 포트(delivery 29094, slack 29099)에서만 열린다. 관리 포트는 Eureka 에 등록되지 않아 Gateway 로 노출되지 않는다.
- actuator 는 `health`, `prometheus` 만 공개한다.
- 지표 이름과 태그에 주문 ID, 주소, Slack ID 같은 개인정보를 넣지 않는다.
- Prometheus, Grafana 포트는 `127.0.0.1` 에만 연다. Grafana 관리자 비밀번호는 파일에 두지 않고 실행할 때 환경변수로 받는다.
- 운영에서는 관리 포트를 내부망(Prometheus)에서만 접근 가능하도록 방화벽으로 제한한다.
