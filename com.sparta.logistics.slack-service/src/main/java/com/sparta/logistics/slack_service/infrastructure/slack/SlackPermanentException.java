package com.sparta.logistics.slack_service.infrastructure.slack;

/**
 * 다시 보내도 성공하지 않는 Slack 발송 실패 (채널 없음, 토큰 오류, 권한 부족 등).
 * 재시도하지 않고 바로 DLT 로 보내, 설정을 고친 뒤 재처리 API 로 다시 보낸다.
 */
public class SlackPermanentException extends RuntimeException {
    public SlackPermanentException(String message) {
        super(message);
    }
}
