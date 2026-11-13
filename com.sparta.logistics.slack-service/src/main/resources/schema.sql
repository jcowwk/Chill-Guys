-- 서비스 시작 시 매번 실행 (spring.sql.init.mode=always). 이미 있으면 아무것도 하지 않는다.
-- DDL_AUTO 가 none 인 환경에서도 이 서비스가 쓰는 테이블과 변경된 컬럼이 반영되도록 한다.

-- 처리한 Kafka 이벤트 ID (중복 처리 방지)
CREATE TABLE IF NOT EXISTS p_processed_event (
    event_id     varchar(64)                    NOT NULL PRIMARY KEY,
    topic        varchar(255)                   NOT NULL,
    processed_at timestamp(6) without time zone NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_processed_event_processed_at ON p_processed_event (processed_at);

-- Slack 알림
CREATE TABLE IF NOT EXISTS p_slack (
    id         uuid                           NOT NULL PRIMARY KEY,
    slack_id   varchar(255),
    text       text,
    created_at timestamp(6) without time zone,
    created_by bigint,
    event_id   varchar(64),
    sent_at    timestamp(6) without time zone
);
-- 예전에 만들어진 p_slack 보정: 알림 문구가 255자를 넘어도 저장되도록, 재시도 시 재사용/재발송 방지 컬럼 추가
ALTER TABLE p_slack ALTER COLUMN text TYPE text;
ALTER TABLE p_slack ADD COLUMN IF NOT EXISTS event_id varchar(64);
ALTER TABLE p_slack ADD COLUMN IF NOT EXISTS sent_at timestamp(6) without time zone;
CREATE UNIQUE INDEX IF NOT EXISTS uk_slack_event_id ON p_slack (event_id);
