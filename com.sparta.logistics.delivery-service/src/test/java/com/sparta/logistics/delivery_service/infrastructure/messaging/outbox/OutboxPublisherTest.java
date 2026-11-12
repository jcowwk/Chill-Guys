package com.sparta.logistics.delivery_service.infrastructure.messaging.outbox;

import com.sparta.logistics.delivery_service.domain.model.OutboxEvent;
import com.sparta.logistics.delivery_service.domain.model.OutboxStatus;
import com.sparta.logistics.delivery_service.domain.repository.OutboxEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class OutboxPublisherTest {

    private OutboxEventRepository repository;
    private KafkaTemplate<String, String> kafkaTemplate;
    private OutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        repository = mock(OutboxEventRepository.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        publisher = new OutboxPublisher(repository, kafkaTemplate, 100, 1000, 1, 5000, 300000);
    }

    private OutboxEvent newEvent() {
        return OutboxEvent.create(UUID.randomUUID(), "HUB", "{\"orderId\":\"x\"}", LocalDateTime.now());
    }

    @Test
    void 발행_성공시_PUBLISHED_로_변경하고_eventId_헤더와_key_를_포함한다() {
        OutboxEvent event = newEvent();
        when(repository.findPublishableForUpdate(any(), anyInt())).thenReturn(List.of(event));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        int published = publisher.publishPendingEvents();

        assertThat(published).isEqualTo(1);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> record = captor.getValue();
        assertThat(record.topic()).isEqualTo("HUB");
        assertThat(record.key()).isEqualTo(event.getAggregateId().toString());
        assertThat(new String(record.headers().lastHeader(OutboxPublisher.EVENT_ID_HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo(event.getId().toString());
    }

    @Test
    void 브로커_장애시_이벤트는_PENDING_으로_남고_나머지_발행은_다음_주기로_미룬다() {
        OutboxEvent first = newEvent();
        OutboxEvent second = newEvent();
        when(repository.findPublishableForUpdate(any(), anyInt())).thenReturn(List.of(first, second));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(
                new KafkaException("send failed", new org.apache.kafka.common.errors.TimeoutException("broker down"))));

        int published = publisher.publishPendingEvents();

        assertThat(published).isZero();
        assertThat(first.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(first.getRetryCount()).isEqualTo(1);
        assertThat(first.getNextRetryAt()).isAfter(LocalDateTime.now());
        assertThat(second.getRetryCount()).isZero();
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void 영구_오류는_재시도_한도_후_FAILED() {
        OutboxEvent event = newEvent();
        when(repository.findPublishableForUpdate(any(), anyInt())).thenReturn(List.of(event));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(
                new KafkaException("send failed", new RecordTooLargeException("too large"))));

        publisher.publishPendingEvents();

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.FAILED);
    }

    @Test
    void 재시도_가능_예외_판별() {
        assertThat(OutboxPublisher.isRetriable(new KafkaException("x",
                new org.apache.kafka.common.errors.TimeoutException("t")))).isTrue();
        assertThat(OutboxPublisher.isRetriable(new java.util.concurrent.TimeoutException())).isTrue();
        assertThat(OutboxPublisher.isRetriable(new KafkaException("x", new RecordTooLargeException("r")))).isFalse();
    }
}
