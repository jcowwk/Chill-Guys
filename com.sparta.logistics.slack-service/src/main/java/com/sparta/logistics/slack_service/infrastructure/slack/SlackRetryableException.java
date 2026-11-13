package com.sparta.logistics.slack_service.infrastructure.slack;

/** 잠시 후 다시 보내면 성공할 수 있는 Slack 발송 실패 (호출 한도, 5xx, 네트워크, 서킷 열림). Kafka 재시도 대상. */
public class SlackRetryableException extends RuntimeException {
    public SlackRetryableException(String message) {
        super(message);
    }

    public SlackRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
