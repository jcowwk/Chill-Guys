package com.sparta.logistics.slack_service.infrastructure.config.kafka;

import com.sparta.logistics.slack_service.application.service.SlackService;
import com.sparta.logistics.slack_service.domain.entity.ProcessedEvent;
import com.sparta.logistics.slack_service.domain.repository.ProcessedEventRepository;
import com.sparta.logistics.slack_service.infrastructure.dto.DeliveryInfoDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ConsumerEndpointTest {

    private SlackService slackService;
    private ProcessedEventRepository processedEventRepository;
    private ConsumerEndpoint endpoint;

    @BeforeEach
    void setUp() {
        slackService = mock(SlackService.class);
        processedEventRepository = mock(ProcessedEventRepository.class);
        endpoint = new ConsumerEndpoint(slackService, processedEventRepository);
    }

    private ConsumerRecord<String, DeliveryInfoDto> record(String eventId) {
        DeliveryInfoDto dto = DeliveryInfoDto.builder().orderId(UUID.randomUUID()).build();
        ConsumerRecord<String, DeliveryInfoDto> record = new ConsumerRecord<>(KafkaTopics.HUB, 0, 0L, "key", dto);
        if (eventId != null) {
            record.headers().add(KafkaTopics.EVENT_ID_HEADER, eventId.getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    @Test
    void 처음_받은_이벤트는_처리하고_처리완료로_기록한다() {
        when(processedEventRepository.existsById("e1")).thenReturn(false);

        endpoint.handle(record("e1"));

        verify(slackService).saveSlackMessage(any());
        verify(processedEventRepository).save(any(ProcessedEvent.class));
    }

    @Test
    void 이미_처리한_이벤트는_중복_발송하지_않는다() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        endpoint.handle(record("e1"));

        verifyNoInteractions(slackService);
        verify(processedEventRepository, never()).save(any());
    }

    @Test
    void eventId_헤더가_없는_기존_메시지도_처리한다() {
        endpoint.handle(record(null));

        verify(slackService).saveSlackMessage(any());
        verify(processedEventRepository, never()).save(any());
    }

    @Test
    void 처리_실패시_예외를_전파하여_재시도_DLT_대상이_되고_처리완료로_기록하지_않는다() {
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        doThrow(new RuntimeException("hub-service down")).when(slackService).saveSlackMessage(any());

        assertThatThrownBy(() -> endpoint.handle(record("e1"))).isInstanceOf(RuntimeException.class);
        verify(processedEventRepository, never()).save(any());
    }

    @Test
    void 처리완료_기록_실패는_재시도를_유발하지_않는다() {
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(processedEventRepository.save(any())).thenThrow(new RuntimeException("db down"));

        endpoint.handle(record("e1")); // 예외 없이 종료

        verify(slackService, times(1)).saveSlackMessage(any());
    }
}
