package com.sparta.logistics.delivery_service.infrastructure.messaging.outbox;

import com.sparta.logistics.delivery_service.application.service.DeliveryAssignmentScheduler;
import com.sparta.logistics.delivery_service.infrastructure.client.CompanyClient;
import com.sparta.logistics.delivery_service.infrastructure.client.DeliveryManagerClient;
import com.sparta.logistics.delivery_service.infrastructure.client.HubRouteClient;
import com.sparta.logistics.delivery_service.infrastructure.client.ProductClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * 실제 PostgreSQL 컨테이너 + Spring 전체 컨텍스트로 Outbox 를 검증하기 위한 공통 설정.
 * 외부 서비스(Feign)와 배송 배정 스케줄러만 mock 으로 대체한다.
 */
abstract class OutboxIntegrationTestSupport {

    @MockitoBean DeliveryManagerClient deliveryManagerClient;
    @MockitoBean CompanyClient companyClient;
    @MockitoBean ProductClient productClient;
    @MockitoBean HubRouteClient hubRouteClient;
    @MockitoBean DeliveryAssignmentScheduler deliveryAssignmentScheduler;

    static void registerCommonProperties(DynamicPropertyRegistry registry, PostgreSQLContainer<?> postgres) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
        registry.add("EUREKA_URL", () -> "localhost:8761");
        registry.add("eureka.client.enabled", () -> "false");
        // 스케줄러 대신 테스트에서 릴레이를 직접 호출해 결정적으로 검증
        registry.add("outbox.relay.fixed-delay-ms", () -> "3600000");
        registry.add("outbox.relay.monitor-delay-ms", () -> "3600000");
        registry.add("outbox.relay.base-backoff-ms", () -> "1000");
        registry.add("outbox.relay.max-backoff-ms", () -> "2000");
    }
}
