package com.sparta.logistics.slack_service.domain;

import com.sparta.logistics.slack_service.domain.entity.ProcessedEvent;
import com.sparta.logistics.slack_service.domain.entity.Slack;
import com.sparta.logistics.slack_service.domain.repository.ProcessedEventRepository;
import com.sparta.logistics.slack_service.domain.repository.SlackRepository;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 팀 환경처럼 DDL_AUTO=none 이어도 시작할 때 schema.sql 로 테이블이 만들어지고,
 * 예전 형태의 p_slack(text varchar(255), 새 컬럼 없음)도 보정되는지 확인한다.
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class SchemaInitIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("spring.kafka.bootstrap-servers", () -> "localhost:1");
        registry.add("EUREKA_URL", () -> "localhost:8761");
        registry.add("eureka.client.enabled", () -> "false");
        registry.add("slack.bot.token", () -> "test-token");
        registry.add("slack.api.url", () -> "http://localhost:1");
    }

    /** Spring 이 뜨기 전에 예전 형태의 p_slack 을 만들어 둔다 */
    @BeforeAll
    static void createLegacySlackTable() throws Exception {
        try (var conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             var stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE p_slack (id uuid PRIMARY KEY, created_at timestamp(6), created_by bigint, "
                    + "slack_id varchar(255), text varchar(255))");
        }
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired SlackRepository slackRepository;
    @Autowired ProcessedEventRepository processedEventRepository;

    @Test
    void DDL_AUTO_none_이어도_필요한_테이블과_컬럼이_준비된다() {
        assertThat(jdbc.queryForObject("SELECT to_regclass('p_processed_event') IS NOT NULL", Boolean.class)).isTrue();
        assertThat(jdbc.queryForObject(
                "SELECT data_type FROM information_schema.columns WHERE table_name='p_slack' AND column_name='text'",
                String.class)).isEqualTo("text");
        assertThat(jdbc.queryForList(
                "SELECT column_name FROM information_schema.columns WHERE table_name='p_slack'", String.class))
                .contains("event_id", "sent_at");

        // 실제로 엔티티 저장이 되는지
        Slack saved = slackRepository.saveAndFlush(Slack.builder().id(UUID.randomUUID()).slackId("U1")
                .text("가".repeat(500)).eventId("e-1").createdAt(new Timestamp(System.currentTimeMillis())).build());
        assertThat(slackRepository.findByEventId("e-1")).get().extracting(Slack::getId).isEqualTo(saved.getId());
        processedEventRepository.saveAndFlush(new ProcessedEvent("e-1", "HUB", LocalDateTime.now()));
        assertThat(processedEventRepository.existsById("e-1")).isTrue();
    }
}
