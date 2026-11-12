package com.sparta.logistics.slack_service.infrastructure.config.kafka;

public final class KafkaTopics {

    public static final String HUB = "HUB";
    public static final String COMPANY = "COMPANY";

    /** 재시도 후에도 처리하지 못한 메시지를 보관하는 Dead Letter Topic 접미사 */
    public static final String DLT_SUFFIX = ".DLT";

    /** delivery-service Outbox 가 넣어 주는 이벤트 식별자 헤더 */
    public static final String EVENT_ID_HEADER = "eventId";

    private KafkaTopics() {
    }
}
