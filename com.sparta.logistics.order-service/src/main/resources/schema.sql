-- 서비스 시작 시 매번 실행 (spring.sql.init.mode=always). DDL_AUTO 가 none 이어도 변경 사항이 반영되도록 한다.
-- 주문 생성 Saga: 재고를 주문 ID 로 차감했는지, 실패 사유
ALTER TABLE IF EXISTS p_order ADD COLUMN IF NOT EXISTS stock_keyed boolean;
ALTER TABLE IF EXISTS p_order ADD COLUMN IF NOT EXISTS failure_reason varchar(500);
-- 상태 값 추가(PROCESSING, FAILED): 예전에 만들어진 상태 제약을 새 값까지 허용하도록 다시 만든다
ALTER TABLE IF EXISTS p_order DROP CONSTRAINT IF EXISTS p_order_status_check;
ALTER TABLE IF EXISTS p_order ADD CONSTRAINT p_order_status_check
    CHECK (status IN ('PROCESSING', 'PENDING', 'CONFIRMED', 'DELIVERED', 'CANCELLED', 'FAILED'));
