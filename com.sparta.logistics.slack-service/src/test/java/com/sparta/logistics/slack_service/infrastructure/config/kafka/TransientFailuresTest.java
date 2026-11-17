package com.sparta.logistics.slack_service.infrastructure.config.kafka;

import feign.FeignException;
import feign.Request;
import feign.RetryableException;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ListenerExecutionFailedException;

import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TransientFailuresTest {

    private static final Request REQUEST = Request.create(Request.HttpMethod.GET, "http://order-service/api/orders/1/info",
            Map.of(), null, StandardCharsets.UTF_8, null);

    @Test
    void 연결_거부와_timeout_과_5xx_는_일시적_실패다() {
        assertThat(TransientFailures.isTransient(
                new ListenerExecutionFailedException("listener", new RetryableException(
                        -1, "Connection refused", Request.HttpMethod.GET, (Long) null, REQUEST)))).isTrue();
        assertThat(TransientFailures.isTransient(new RuntimeException(new ConnectException("refused")))).isTrue();
        assertThat(TransientFailures.isTransient(
                new FeignException.ServiceUnavailable("no instance", REQUEST, null, null))).isTrue();
    }

    @Test
    void 요청_오류와_알_수_없는_오류는_일시적_실패가_아니다() {
        assertThat(TransientFailures.isTransient(
                new FeignException.NotFound("not found", REQUEST, null, null))).isFalse();
        assertThat(TransientFailures.isTransient(new IllegalStateException("bug"))).isFalse();
    }
}
