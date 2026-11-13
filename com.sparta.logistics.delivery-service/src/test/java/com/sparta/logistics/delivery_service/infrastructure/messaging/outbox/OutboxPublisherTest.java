package com.sparta.logistics.delivery_service.infrastructure.messaging.outbox;

import com.sparta.logistics.delivery_service.domain.model.OutboxEvent;
import com.sparta.logistics.delivery_service.domain.model.OutboxStatus;
import com.sparta.logistics.delivery_service.domain.repository.OutboxEventRepository;
import com.sparta.logistics.delivery_service.infrastructure.monitoring.DeliveryPipelineMetrics;
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
    private final DeliveryPipelineMetrics metrics = new DeliveryPipelineMetrics(
            new io.micrometer.core.instrument.simple.SimpleMeterRegistry(), null, null);

    @BeforeEach
    void setUp() {
        repository = mock(OutboxEventRepository.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        publisher = new OutboxPublisher(repository, kafkaTemplate, metrics, 100, 1000, 1, 5000, 300000);
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

        PublishResult result = publisher.publishPendingEvents();

        assertThat(result.published()).isEqualTo(1);
        assertThat(result.failed()).isFalse();
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
    void 브로커_응답_실패시_모든_이벤트는_PENDING_으로_남아_재시도된다() {
        OutboxEvent first = newEvent();
        OutboxEvent second = newEvent();
        when(repository.findPublishableForUpdate(any(), anyInt())).thenReturn(List.of(first, second));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(
                new KafkaException("send failed", new org.apache.kafka.common.errors.TimeoutException("broker down"))));

        PublishResult result = publisher.publishPendingEvents();

        assertThat(result.published()).isZero();
        assertThat(result.failed()).isTrue();
        assertThat(result.hasMore()).isFalse();
        for (OutboxEvent e : List.of(first, second)) {
            assertThat(e.getStatus()).isEqualTo(OutboxStatus.PENDING);
            assertThat(e.getRetryCount()).isEqualTo(1);
            assertThat(e.getNextRetryAt()).isAfter(LocalDateTime.now());
        }
    }

    @Test
    void 브로커_연결_불가로_send_가_즉시_실패하면_나머지는_보내지_않고_다음_주기로_미룬다() {
        OutboxEvent first = newEvent();
        OutboxEvent second = newEvent();
        when(repository.findPublishableForUpdate(any(), anyInt())).thenReturn(List.of(first, second));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(
                new KafkaException("metadata", new org.apache.kafka.common.errors.TimeoutException("not present in metadata")));

        PublishResult result = publisher.publishPendingEvents();

        assertThat(result.failed()).isTrue();
        assertThat(first.getRetryCount()).isEqualTo(1);
        assertThat(second.getRetryCount()).isZero();
        assertThat(second.getStatus()).isEqualTo(OutboxStatus.PENDING);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void 배치가_가득차면_hasMore_로_연속_발행을_알린다() {
        publisher = new OutboxPublisher(repository, kafkaTemplate, metrics, 2, 1000, 1, 5000, 300000);
        when(repository.findPublishableForUpdate(any(), anyInt())).thenReturn(List.of(newEvent(), newEvent()));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        assertThat(publisher.publishPendingEvents().hasMore()).isTrue();
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
