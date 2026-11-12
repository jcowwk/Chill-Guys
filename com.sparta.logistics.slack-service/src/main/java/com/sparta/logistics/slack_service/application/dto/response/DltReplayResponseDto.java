package com.sparta.logistics.slack_service.application.dto.response;

import java.util.List;

/**
 * DLT 조회/재처리 결과.
 *
 * @param topic     원본 토픽 (HUB / COMPANY)
 * @param dryRun    true 면 조회만 하고 재투입·오프셋 커밋을 하지 않음
 * @param scanned   이번에 읽은 DLT 메시지 수
 * @param replayed  원본 토픽으로 재투입한 수
 * @param skipped   재투입하지 않은 수 (역직렬화 불가 / 재처리 한도 초과)
 * @param messages  메시지별 상세
 */
public record DltReplayResponseDto(String topic, boolean dryRun, int scanned, int replayed, int skipped,
                                   List<Item> messages) {

    public record Item(int partition, long offset, String eventId, String key, Action action,
                       String reason, String lastError, int replayCount) {
    }

    public enum Action {
        REPLAYED,        // 원본 토픽으로 재투입
        WOULD_REPLAY,    // dryRun: 재투입 대상
        SKIPPED_POISON,  // 역직렬화 불가 메시지 - 재투입해도 실패하므로 제외
        SKIPPED_LIMIT    // 재처리 한도 초과 - 원인 미해결로 판단, 무한 루프 방지
    }
}
