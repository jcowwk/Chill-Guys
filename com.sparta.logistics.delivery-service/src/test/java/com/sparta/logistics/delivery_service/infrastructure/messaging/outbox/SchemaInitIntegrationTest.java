package com.sparta.logistics.delivery_service.infrastructure.messaging.outbox;

import com.sparta.logistics.delivery_service.domain.repository.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** 팀 환경처럼 DDL_AUTO=none 이어도 시작할 때 schema.sql 로 Outbox 테이블이 만들어지는지 확인한다. */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class SchemaInitIntegrationTest extends OutboxIntegrationTestSupport {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registerCommonProperties(registry, postgres);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("spring.kafka.bootstrap-servers", () -> "localhost:1");
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired OutboxEventRecorder recorder;
    @Autowired OutboxEventRepository repository;
    @Autowired TransactionTemplate tx;

    @Test
    void DDL_AUTO_none_이어도_Outbox_테이블이_만들어지고_저장된다() {
        assertThat(jdbc.queryForObject("SELECT to_regclass('p_outbox_event') IS NOT NULL", Boolean.class)).isTrue();

        tx.executeWithoutResult(s -> recorder.record(UUID.randomUUID(), "HUB", Map.of("k", "v")));

        assertThat(repository.count()).isEqualTo(1);
    }
}
