package com.sparta.logistics.order_service;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * 실제 PostgreSQL 로 애플리케이션 전체가 뜨는지 확인한다.
 * (시작할 때 schema.sql 을 실행하므로 DB 연결이 필요하다)
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class OrderServiceApplicationTests {

	@Container
	static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", postgres::getJdbcUrl);
		registry.add("spring.datasource.username", postgres::getUsername);
		registry.add("spring.datasource.password", postgres::getPassword);
		registry.add("DDL_AUTO", () -> "create-drop");
		registry.add("EUREKA_URL", () -> "localhost:8761");
		registry.add("eureka.client.enabled", () -> "false");
	}

	@Test
	void contextLoads() {
	}

}
