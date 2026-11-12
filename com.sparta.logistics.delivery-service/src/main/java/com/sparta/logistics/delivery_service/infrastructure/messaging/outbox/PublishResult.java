package com.sparta.logistics.delivery_service.infrastructure.messaging.outbox;

/**
 * 한 배치의 발행 결과.
 *
 * @param fetched   이번 배치에서 잠금 획득한 이벤트 수
 * @param published 발행 성공 수
 * @param failed    발행 실패가 있었는지 (브로커 장애 의심 → 이번 주기 drain 중단)
 * @param batchSize 배치 크기 (fetched == batchSize 면 아직 남은 이벤트가 있을 수 있음)
 */
public record PublishResult(int fetched, int published, boolean failed, int batchSize) {

    public static PublishResult empty(int batchSize) {
        return new PublishResult(0, 0, false, batchSize);
    }

    public boolean hasMore() {
        return !failed && fetched >= batchSize;
    }
}
