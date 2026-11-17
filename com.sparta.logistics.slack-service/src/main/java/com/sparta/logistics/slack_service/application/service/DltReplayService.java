package com.sparta.logistics.slack_service.application.service;

import com.sparta.logistics.slack_service.application.dto.response.DltReplayResponseDto;
import com.sparta.logistics.slack_service.application.dto.response.DltReplayResponseDto.Action;
import com.sparta.logistics.slack_service.application.dto.response.DltReplayResponseDto.Item;
import com.sparta.logistics.slack_service.infrastructure.config.kafka.KafkaTopics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * DLT(<topic>.DLT)에 격리된 메시지를 조회하고, 원인을 해결한 뒤 원본 토픽으로 재투입한다.
 *
 * - 수동 재처리(replay): 운영자가 원인을 해결한 뒤 실행한다.
 * - 자동 재처리(autoReplay): 스케줄러가 주기적으로 실행한다. 내부 서비스가 오래 멈췄다 돌아온 경우처럼
 *   시간이 지나면 풀리는 실패를 사람 없이 다시 처리한다. 다시 보내도 성공하지 않는 Slack 오류(채널 없음 등)는
 *   재처리 횟수를 쓰지 않도록 건너뛰고, 수동 재처리에 남겨 둔다 (자동과 수동은 오프셋을 따로 관리).
 * - 원인이 그대로면 원본 → 실패 → DLT 로 돌기 때문에 재처리 횟수 한도를 둔다.
 * - eventId 헤더를 유지하므로, 이미 처리된 메시지는 컨슈머의 멱등 처리로 걸러진다.
 * - 재처리 횟수 헤더(x-replay-count)를 붙이고 한도를 넘으면 더 이상 재투입하지 않는다.
 * - 역직렬화 불가(poison) 메시지는 재투입해도 실패하므로 제외하고 결과로만 알린다.
 * - 재투입(또는 의도적 제외)이 끝난 메시지까지만 오프셋을 커밋하므로 같은 메시지를 두 번 재투입하지 않는다.
 */
@Slf4j
@Service
public class DltReplayService {

    public static final String REPLAY_COUNT_HEADER = "x-replay-count";
    private static final Set<String> ALLOWED_TOPICS = Set.of(KafkaTopics.HUB, KafkaTopics.COMPANY);
    private static final String DLT_HEADER_PREFIX = "kafka_dlt-";
    private static final String PERMANENT_EXCEPTION = "SlackPermanentException";

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final String bootstrapServers;
    private final int maxReplayCount;
    private final long pollTimeoutMs;
    private final ReentrantLock lock = new ReentrantLock();

    public DltReplayService(@Qualifier("deadLetterKafkaTemplate") KafkaTemplate<Object, Object> kafkaTemplate,
                            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                            @Value("${kafka.dlt.replay.max-replay-count:3}") int maxReplayCount,
                            @Value("${kafka.dlt.replay.poll-timeout-ms:3000}") long pollTimeoutMs) {
        this.kafkaTemplate = kafkaTemplate;
        this.bootstrapServers = bootstrapServers;
        this.maxReplayCount = maxReplayCount;
        this.pollTimeoutMs = pollTimeoutMs;
    }

    /** 재투입 대상만 조회한다 (오프셋 커밋 없음 → 몇 번을 호출해도 같은 결과). */
    public DltReplayResponseDto peek(String topic, int max) {
        return run(topic, max, true, false);
    }

    /** 원본 토픽으로 재투입하고 처리한 지점까지 오프셋을 커밋한다. */
    public DltReplayResponseDto replay(String topic, int max) {
        return run(topic, max, false, false);
    }

    /** 스케줄러용: 다시 보내도 성공하지 않는 Slack 오류를 빼고 재투입한다. 자동 재처리 전용 오프셋을 쓴다. */
    public DltReplayResponseDto autoReplay(String topic, int max) {
        return run(topic, max, false, true);
    }

    private DltReplayResponseDto run(String topic, int max, boolean dryRun, boolean auto) {
        if (!ALLOWED_TOPICS.contains(topic)) {
            throw new IllegalArgumentException("지원하지 않는 토픽입니다: " + topic + " (허용: " + ALLOWED_TOPICS + ")");
        }
        if (max < 1 || max > 1000) {
            throw new IllegalArgumentException("max 는 1~1000 사이여야 합니다: " + max);
        }
        // 같은 인스턴스에서 동시 실행되면 같은 메시지를 중복 재투입할 수 있으므로 직렬화
        if (!lock.tryLock()) {
            throw new IllegalStateException("이미 DLT 재처리가 진행 중입니다. 잠시 후 다시 시도하세요.");
        }
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps(topic, max, auto))) {
            return process(consumer, topic, max, dryRun, auto);
        } finally {
            lock.unlock();
        }
    }

    private DltReplayResponseDto process(KafkaConsumer<String, byte[]> consumer, String topic, int max,
                                         boolean dryRun, boolean auto) {
        String dltTopic = topic + KafkaTopics.DLT_SUFFIX;
        List<PartitionInfo> partitions = consumer.partitionsFor(dltTopic, Duration.ofMillis(pollTimeoutMs));
        if (partitions == null || partitions.isEmpty()) {
            return new DltReplayResponseDto(topic, dryRun, 0, 0, 0, List.of());
        }
        // 그룹 리밸런스 없이 바로 읽도록 assign (오프셋은 group.id 의 커밋 지점부터)
        List<TopicPartition> tps = partitions.stream().map(p -> new TopicPartition(dltTopic, p.partition())).toList();
        consumer.assign(tps);
        // 실행 시작 시점의 끝 위치까지만 처리한다.
        // 그러지 않으면 방금 재투입한 메시지가 또 실패해 DLT 로 돌아왔을 때 같은 실행에서 다시 재투입하게 된다.
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);

        List<Item> items = new ArrayList<>();
        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        int replayed = 0;
        int skipped = 0;
        long deadline = System.currentTimeMillis() + pollTimeoutMs;

        outer:
        while (items.size() < max && !caughtUp(consumer, endOffsets) && System.currentTimeMillis() < deadline) {
            for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(300))) {
                if (items.size() >= max) break outer;
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                if (record.offset() >= endOffsets.getOrDefault(tp, 0L)) continue; // 실행 이후 들어온 메시지는 다음 실행에서
                Item item = handle(record, topic, dryRun, auto);
                items.add(item);
                if (item.action() == Action.REPLAYED || item.action() == Action.WOULD_REPLAY) replayed++;
                else skipped++;
                toCommit.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1));
            }
        }

        if (!dryRun && !toCommit.isEmpty()) {
            consumer.commitSync(toCommit);
        }
        if (!dryRun) {
            if (auto && items.isEmpty()) {
                log.debug("[DLT] 자동 재처리 대상 없음 topic={}", topic);
            } else {
                log.warn("[DLT] {}재처리 실행 topic={}, 재투입={}, 제외={}", auto ? "자동 " : "", topic, replayed, skipped);
            }
        }
        return new DltReplayResponseDto(topic, dryRun, items.size(), replayed, skipped, items);
    }

    private Item handle(ConsumerRecord<String, byte[]> record, String topic, boolean dryRun, boolean auto) {
        Headers headers = record.headers();
        String eventId = text(headers, KafkaTopics.EVENT_ID_HEADER);
        String fqcn = text(headers, KafkaHeaders.DLT_EXCEPTION_FQCN);
        String causeFqcn = text(headers, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN);
        String lastError = text(headers, KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        int replayCount = replayCount(headers);

        if (contains(fqcn, "DeserializationException") || contains(causeFqcn, "DeserializationException")) {
            return item(record, eventId, Action.SKIPPED_POISON, "역직렬화 불가 - 원본 메시지 형식 확인 필요", lastError, replayCount);
        }
        if (auto && (contains(fqcn, PERMANENT_EXCEPTION) || contains(causeFqcn, PERMANENT_EXCEPTION))) {
            return item(record, eventId, Action.SKIPPED_PERMANENT,
                    "다시 보내도 성공하지 않는 Slack 오류 - 원인 해결 후 수동 재처리", lastError, replayCount);
        }
        if (replayCount >= maxReplayCount) {
            return item(record, eventId, Action.SKIPPED_LIMIT,
                    "재처리 " + replayCount + "회 초과 - 원인 해결 여부 확인 필요", lastError, replayCount);
        }
        if (dryRun) {
            return item(record, eventId, Action.WOULD_REPLAY, null, lastError, replayCount);
        }

        ProducerRecord<Object, Object> out = new ProducerRecord<>(topic, record.key(), record.value());
        for (Header h : headers) {
            // 이전 실패 정보(kafka_dlt-*)는 떼고 eventId 등 원본 헤더만 유지
            if (!h.key().startsWith(DLT_HEADER_PREFIX) && !h.key().equals(REPLAY_COUNT_HEADER)) {
                out.headers().add(h);
            }
        }
        out.headers().add(REPLAY_COUNT_HEADER, ByteBuffer.allocate(4).putInt(replayCount + 1).array());
        try {
            kafkaTemplate.send(out).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            // 재투입에 실패하면 오프셋을 커밋하지 않도록 예외로 전체를 중단한다
            throw new IllegalStateException("DLT 재투입 실패 offset=" + record.offset() + ": " + e.getMessage(), e);
        }
        return item(record, eventId, Action.REPLAYED, null, lastError, replayCount + 1);
    }

    private static boolean caughtUp(KafkaConsumer<?, ?> consumer, Map<TopicPartition, Long> endOffsets) {
        return endOffsets.entrySet().stream().allMatch(e -> consumer.position(e.getKey()) >= e.getValue());
    }

    private Map<String, Object> consumerProps(String topic, int max, boolean auto) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, (auto ? "slack-dlt-auto-replay-" : "slack-dlt-replay-") + topic);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, max);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return props;
    }

    private static Item item(ConsumerRecord<String, byte[]> r, String eventId, Action action, String reason,
                             String lastError, int replayCount) {
        return new Item(r.partition(), r.offset(), eventId, r.key(), action, reason, lastError, replayCount);
    }

    private static int replayCount(Headers headers) {
        Header h = headers.lastHeader(REPLAY_COUNT_HEADER);
        return h == null || h.value().length != 4 ? 0 : ByteBuffer.wrap(h.value()).getInt();
    }

    private static String text(Headers headers, String key) {
        Header h = headers.lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static boolean contains(String value, String token) {
        return value != null && value.contains(token);
    }
}
