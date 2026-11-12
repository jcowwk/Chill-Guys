package com.sparta.logistics.delivery_service.load;

import com.sparta.logistics.delivery_service.application.service.DeliveryAssignmentScheduler;
import com.sparta.logistics.delivery_service.domain.model.OutboxStatus;
import com.sparta.logistics.delivery_service.domain.repository.OutboxEventRepository;
import com.sparta.logistics.delivery_service.infrastructure.client.CompanyClient;
import com.sparta.logistics.delivery_service.infrastructure.client.DeliveryManagerClient;
import com.sparta.logistics.delivery_service.infrastructure.client.HubRouteClient;
import com.sparta.logistics.delivery_service.infrastructure.client.ProductClient;
import com.sparta.logistics.delivery_service.infrastructure.messaging.dto.DeliveryInfoDto;
import com.sparta.logistics.delivery_service.infrastructure.messaging.outbox.OutboxEventRecorder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * [부하 테스트] 배송 이벤트가 대량으로 발생할 때 Outbox 경로의 처리량(TPS)과 지연을 측정한다.
 * 실제 스케줄러 설정(application.yml 의 outbox.relay.*)을 그대로 사용한다.
 *
 * 실행: ./gradlew loadTest  (환경변수 LOAD_EVENTS, LOAD_THREADS 로 규모 조정)
 */
@Tag("load")
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class OutboxLoadTest {

    private static final int EVENTS = Integer.parseInt(System.getenv().getOrDefault("LOAD_EVENTS", "3000"));
    private static final int THREADS = Integer.parseInt(System.getenv().getOrDefault("LOAD_THREADS", "16"));

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.1");

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("EUREKA_URL", () -> "localhost:8761");
        registry.add("eureka.client.enabled", () -> "false");
        registry.add("logging.level.root", () -> "WARN");
        // 튜닝 실험용: LOAD_BATCH_SIZE=500 ./gradlew loadTest
        String batchSize = System.getenv("LOAD_BATCH_SIZE");
        if (batchSize != null) registry.add("outbox.relay.batch-size", () -> batchSize);
    }

    @MockitoBean DeliveryManagerClient deliveryManagerClient;
    @MockitoBean CompanyClient companyClient;
    @MockitoBean ProductClient productClient;
    @MockitoBean HubRouteClient hubRouteClient;
    @MockitoBean DeliveryAssignmentScheduler deliveryAssignmentScheduler;

    @Autowired OutboxEventRecorder recorder;
    @Autowired OutboxEventRepository repository;
    @Autowired TransactionTemplate tx;
    @Autowired JdbcTemplate jdbc;

    @Test
    void 대량_배송_이벤트_발생시_Outbox_쓰기_TPS_와_Kafka_발행_TPS() throws Exception {
        // 1) 쓰기 경로: 여러 스레드가 동시에 "배송 상태 변경 트랜잭션 + Outbox 기록" 수행
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        AtomicInteger errors = new AtomicInteger();
        List<Long> txLatenciesMicros = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch done = new CountDownLatch(EVENTS);

        long writeStart = System.nanoTime();
        for (int i = 0; i < EVENTS; i++) {
            pool.submit(() -> {
                long s = System.nanoTime();
                try {
                    tx.executeWithoutResult(status -> recorder.record(UUID.randomUUID(), "HUB", sampleDto()));
                    txLatenciesMicros.add((System.nanoTime() - s) / 1000);
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }
        done.await(5, TimeUnit.MINUTES);
        double writeSec = (System.nanoTime() - writeStart) / 1e9;
        pool.shutdown();

        // 2) 발행 경로: 스케줄러(OutboxRelay)가 전부 발행할 때까지 대기
        long drainDeadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
        while (repository.countByStatus(OutboxStatus.PENDING) > 0 && System.currentTimeMillis() < drainDeadline) {
            Thread.sleep(200);
        }
        long remaining = repository.countByStatus(OutboxStatus.PENDING);

        Map<String, Object> pub = jdbc.queryForMap("""
                SELECT EXTRACT(EPOCH FROM (MAX(published_at) - MIN(created_at))) AS span_sec,
                       PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (published_at - created_at))) AS p50,
                       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (published_at - created_at))) AS p95,
                       MAX(EXTRACT(EPOCH FROM (published_at - created_at))) AS max_lag
                FROM p_outbox_event WHERE status = 'PUBLISHED'
                """);
        long published = repository.countByStatus(OutboxStatus.PUBLISHED);
        double spanSec = ((Number) pub.get("span_sec")).doubleValue();

        List<Long> sorted = new ArrayList<>(txLatenciesMicros);
        Collections.sort(sorted);

        System.out.println();
        System.out.println("================ [LOAD] Outbox 부하 테스트 결과 ================");
        System.out.printf("이벤트 수: %d, 동시 스레드: %d, relay batch-size: %s%n", EVENTS, THREADS,
                System.getenv().getOrDefault("LOAD_BATCH_SIZE", "100(기본)"));
        System.out.printf("[쓰기] 배송Tx+Outbox 기록 TPS: %.0f tx/s (소요 %.2fs, 실패 %d)%n", EVENTS / writeSec, writeSec, errors.get());
        System.out.printf("[쓰기] 트랜잭션 지연 p50=%.1fms p95=%.1fms max=%.1fms%n",
                pct(sorted, 0.5) / 1000.0, pct(sorted, 0.95) / 1000.0, sorted.get(sorted.size() - 1) / 1000.0);
        System.out.printf("[발행] Kafka 발행 완료: %d건, 미발행: %d건%n", published, remaining);
        System.out.printf("[발행] 실효 발행 TPS: %.0f events/s (첫 생성 → 마지막 발행 %.1fs)%n", published / spanSec, spanSec);
        System.out.printf("[발행] 생성→발행 지연 p50=%.2fs p95=%.2fs max=%.2fs%n",
                num(pub.get("p50")), num(pub.get("p95")), num(pub.get("max_lag")));
        System.out.println("==============================================================");

        assertThat(errors.get()).isZero();
        assertThat(remaining).as("모든 이벤트가 유실 없이 발행되어야 함").isZero();
    }

    private static long pct(List<Long> sorted, double p) {
        return sorted.get((int) Math.min(sorted.size() - 1, Math.ceil(p * sorted.size()) - 1));
    }

    private static double num(Object o) {
        return o == null ? 0 : ((Number) o).doubleValue();
    }

    private static DeliveryInfoDto sampleDto() {
        return DeliveryInfoDto.builder()
                .orderId(UUID.randomUUID()).departureHubId(UUID.randomUUID()).destinationHubId(UUID.randomUUID())
                .productId(UUID.randomUUID()).deliveryManagerId(1L).slackId("U-LOAD").address("서울시 강남구")
                .build();
    }
}
