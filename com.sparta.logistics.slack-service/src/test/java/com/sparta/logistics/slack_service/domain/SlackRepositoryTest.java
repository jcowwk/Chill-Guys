package com.sparta.logistics.slack_service.domain;

import com.sparta.logistics.slack_service.domain.entity.Slack;
import com.sparta.logistics.slack_service.domain.repository.SlackRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 실제 PostgreSQL 에서 긴 알림 문구가 저장되는지 확인한다.
 * (varchar(255) 매핑일 때 실제 주소/허브명이 들어간 알림이 전부 저장 실패 → DLT 로 가던 버그의 회귀 방지)
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Testcontainers(disabledWithoutDocker = true)
class SlackRepositoryTest {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
    }

    @Autowired SlackRepository slackRepository;

    @Test
    void 실제_길이의_알림_문구가_255자를_넘어도_저장된다() {
        String text = """
                배송지: 서울특별시 강남구 테헤란로 427 위워크타워 10층 물류센터 수령 데스크
                출발 허브 이름: 서울특별시 센터
                출발 허브 주소: 서울특별시 송파구 송파대로 55
                도착 허브 이름: 경기 남부 센터
                도착 허브 주소: 경기도 이천시 덕평로 257-21
                주문 ID: %s
                상품 ID: %s
                상품명: 마른 오징어 대용량 업소용 1kg 묶음
                수량: 3
                AI 예상 배송 날짜: 2026-11-14에 도착할 예정입니다. 출발 허브에서 도착 허브까지 약 1시간 30분이 소요됩니다.
                """.formatted(UUID.randomUUID(), UUID.randomUUID()).repeat(3);
        assertThat(text.length()).isGreaterThan(255);

        Slack saved = slackRepository.saveAndFlush(Slack.builder()
                .id(UUID.randomUUID()).slackId("U-1").text(text)
                .createdAt(new Timestamp(System.currentTimeMillis())).build());

        assertThat(slackRepository.findById(saved.getId()).orElseThrow().getText()).isEqualTo(text);
    }
}
