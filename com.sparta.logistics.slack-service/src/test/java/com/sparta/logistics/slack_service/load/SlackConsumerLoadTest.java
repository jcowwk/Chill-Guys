package com.sparta.logistics.slack_service.load;

import com.sparta.logistics.slack_service.infrastructure.config.kafka.KafkaTopics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * [부하 테스트] 배송 알림 컨슈머(slack-service)의 처리량(TPS)/지연과,
 * AI(Gemini) 가 느려지거나 장애일 때의 영향을 실제 Kafka + 실제 SlackService 코드로 측정한다.
 * 외부 API 만 스텁 서버로 대체 (hub/order/product/ai-service, Slack Web API).
 *
 * 실행: ./gradlew loadTest  (환경변수 LOAD_MESSAGES, LOAD_PARTITIONS 로 규모 조정)
 */
@Tag("load")
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SlackConsumerLoadTest {

    private static final int MESSAGES = Integer.parseInt(System.getenv().getOrDefault("LOAD_MESSAGES", "1000"));
    private static final int PARTITIONS = Integer.parseInt(System.getenv().getOrDefault("LOAD_PARTITIONS", "3"));

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.1");

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    static final ExternalApiStubServer stub;

    static {
        try {
            stub = new ExternalApiStubServer();
            stub.start();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static KafkaProducer<String, String> producer;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
        registry.add("spring.jpa.show-sql", () -> "false");
        registry.add("logging.level.root", () -> "WARN");
        registry.add("EUREKA_URL", () -> "localhost:8761");
        registry.add("eureka.client.enabled", () -> "false");
        registry.add("slack.bot.token", () -> "load-test");
        registry.add("slack.api.url", stub::baseUrl);
        // Eureka 대신 스텁 서버로 라우팅
        for (String service : List.of("hub-service", "order-service", "product-service", "ai-service")) {
            registry.add("spring.cloud.discovery.client.simple.instances." + service + "[0].uri", stub::baseUrl);
        }
    }

    @BeforeAll
    static void setUp() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(KafkaTopics.HUB, PARTITIONS, (short) 1))).all().get();
        }
        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.LINGER_MS_CONFIG, 5));
    }

    @AfterAll
    static void tearDown() {
        producer.close();
        stub.stop();
    }

    @BeforeEach
    void resetStub() {
        stub.reset();
    }

    @Test
    @Order(1)
    void 시나리오1_정상상태_대량_알림_처리량() throws Exception {
        stub.aiDelayMs = 50;   // 정상 시 내부 API + AI 응답
        Result r = burst(MESSAGES, Duration.ofMinutes(10));
        r.print("시나리오1: 정상 (AI 응답 50ms), " + MESSAGES + "건 버스트");
    }

    @Test
    @Order(2)
    void 시나리오2_Gemini_응답이_1초로_느려질때() throws Exception {
        stub.aiDelayMs = 1000;
        Result r = burst(60, Duration.ofMinutes(5));
        r.print("시나리오2: AI(Gemini) 응답 1s, 60건 버스트");
    }

    @Test
    @Order(3)
    void 시나리오3_Gemini_가_15초씩_걸리는_심각한_지연() throws Exception {
        stub.aiDelayMs = 15_000;
        Result r = burst(6, Duration.ofMinutes(5));
        r.print("시나리오3: AI(Gemini) 응답 15s (Feign 기본 readTimeout 60s 이내), 6건");
    }

    @Test
    @Order(4)
    void 시나리오4_Gemini_장애_500_에러() throws Exception {
        stub.aiFailing = true;
        int n = 5;
        Result r = burst(n, Duration.ofSeconds(60));
        long dlt = countDlt();
        r.print("시나리오4: AI(Gemini) 장애 - 500 응답, " + n + "건");
        System.out.printf("  → AI 호출 횟수: %d (건당 %.1f회, 재시도 포함), Slack 발송: %d건, DLT 격리: %d건%n",
                stub.aiCalls.get(), stub.aiCalls.get() / (double) n, stub.slackMessages.get(), dlt);
    }

    // ---------------------------------------------------------------------------------------------

    private Result burst(int count, Duration timeout) throws Exception {
        Map<UUID, Long> sentAt = new HashMap<>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            UUID orderId = UUID.randomUUID();
            ProducerRecord<String, String> record = new ProducerRecord<>(KafkaTopics.HUB, UUID.randomUUID().toString(), json(orderId));
            record.headers().add(KafkaTopics.EVENT_ID_HEADER, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
            sentAt.put(orderId, System.currentTimeMillis());
            producer.send(record);
        }
        producer.flush();
        long produced = System.currentTimeMillis();

        long deadline = start + timeout.toMillis();
        while (System.currentTimeMillis() < deadline && delivered(sentAt) < count) {
            Thread.sleep(100);
        }
        // DLT 처리 등 마무리 대기
        if (delivered(sentAt) < count) Thread.sleep(2000);
        return new Result(count, sentAt, Map.copyOf(stub.slackReceivedAt), start, produced, System.currentTimeMillis());
    }

    private static long delivered(Map<UUID, Long> sentAt) {
        return sentAt.keySet().stream().filter(stub.slackReceivedAt::containsKey).count();
    }

    private static long countDlt() {
        try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-count-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class))) {
            c.subscribe(List.of(KafkaTopics.HUB + KafkaTopics.DLT_SUFFIX));
            long n = 0;
            long end = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < end) {
                n += c.poll(Duration.ofMillis(500)).count();
            }
            return n;
        }
    }

    private static String json(UUID orderId) {
        return """
                {"departureHubId":"%s","destinationHubId":"%s","deliveryManagerId":7,"productId":"%s",\
                "orderId":"%s","slackId":"U-LOAD","address":"서울시 강남구"}"""
                .formatted(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), orderId);
    }

    private record Result(int count, Map<UUID, Long> sentAt, Map<UUID, Long> receivedAt,
                          long start, long produced, long end) {
        void print(String title) {
            List<Long> latencies = new ArrayList<>();
            long last = start;
            for (var e : sentAt.entrySet()) {
                Long r = receivedAt.get(e.getKey());
                if (r != null) {
                    latencies.add(r - e.getValue());
                    last = Math.max(last, r);
                }
            }
            Collections.sort(latencies);
            int ok = latencies.size();
            double spanSec = Math.max(1, last - start) / 1000.0;
            System.out.println();
            System.out.println("================ [LOAD] " + title + " ================");
            System.out.printf("  발행 %d건 (발행 소요 %dms), 파티션 %d%n", count, produced - start, PARTITIONS);
            System.out.printf("  Slack 알림 도착: %d/%d건 (미도착 %d건)%n", ok, count, count - ok);
            if (ok > 0) {
                System.out.printf("  처리량: %.1f 건/s (첫 발행 → 마지막 알림 %.1fs)%n", ok / spanSec, spanSec);
                System.out.printf("  발행→알림 지연 p50=%dms p95=%dms max=%dms%n",
                        latencies.get((int) Math.max(0, Math.ceil(ok * 0.5) - 1)),
                        latencies.get((int) Math.max(0, Math.ceil(ok * 0.95) - 1)),
                        latencies.get(ok - 1));
            }
            System.out.println("==============================================================");
        }
    }
}
