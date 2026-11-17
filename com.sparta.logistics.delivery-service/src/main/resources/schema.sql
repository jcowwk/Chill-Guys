-- 서비스 시작 시 매번 실행 (spring.sql.init.mode=always). 이미 있으면 아무것도 하지 않는다.
-- DDL_AUTO 가 none 인 환경에서도 이 서비스가 쓰는 새 테이블이 만들어지도록 한다.

-- Transactional Outbox: 배송 상태 변경과 같은 트랜잭션으로 저장되는 "보낼 메시지"
CREATE TABLE IF NOT EXISTS p_outbox_event (
    id            uuid                           NOT NULL PRIMARY KEY,
    aggregate_id  uuid                           NOT NULL,
    topic         varchar(255)                   NOT NULL,
    message_key   varchar(255)                   NOT NULL,
    payload       text                           NOT NULL,
    status        varchar(20)                    NOT NULL CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    retry_count   integer                        NOT NULL,
    next_retry_at timestamp(6) without time zone NOT NULL,
    last_error    varchar(1000),
    created_at    timestamp(6) without time zone NOT NULL,
    published_at  timestamp(6) without time zone
);
CREATE INDEX IF NOT EXISTS idx_outbox_status_next_retry ON p_outbox_event (status, next_retry_at);

-- 주문 단위 배송 취소 표시: 취소가 배송 생성보다 먼저 끝난 경우 늦게 온 생성을 거절한다
CREATE TABLE IF NOT EXISTS p_delivery_cancellation (
    order_id     uuid                           NOT NULL PRIMARY KEY,
    cancelled_at timestamp(6) without time zone NOT NULL
);
