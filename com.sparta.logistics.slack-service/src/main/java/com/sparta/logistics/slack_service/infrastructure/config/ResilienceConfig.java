package com.sparta.logistics.slack_service.infrastructure.config;

import com.sparta.logistics.slack_service.infrastructure.slack.SlackRateLimitedException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JCircuitBreakerFactory;
import org.springframework.cloud.client.circuitbreaker.Customizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * 외부 API(AI 서비스 → Gemini, Slack) 호출 장애가 배송 알림 전체로 번지지 않도록
 * 서킷 브레이커 + 시간 제한을 설정한다.
 *
 * - 최근 10건 중 50% 이상 실패하면 OPEN → 30초 동안 호출 없이 즉시 fallback
 * - TimeLimiter 는 Feign readTimeout(application.yml) 보다 길게 잡아,
 *   정상적인 느린 응답은 Feign 이 판단하고 TimeLimiter 는 최후의 상한으로만 동작하게 한다.
 */
@Configuration
public class ResilienceConfig {

    public static final String AI = "ai";
    public static final String SLACK = "slack";

    @Bean
    public Customizer<Resilience4JCircuitBreakerFactory> externalApiCircuitBreakerCustomizer() {
        return factory -> {
            factory.configure(builder -> builder
                    .circuitBreakerConfig(circuitBreakerConfig())
                    .timeLimiterConfig(timeLimiter(Duration.ofSeconds(8))), AI);

            factory.configure(builder -> builder
                    .circuitBreakerConfig(slackCircuitBreakerConfig())
                    .timeLimiterConfig(timeLimiter(Duration.ofSeconds(5))), SLACK);
        };
    }

    /**
     * Slack 은 호출 한도 초과(429)를 장애로 보지 않는다.
     * 429 까지 실패로 세면, 한 채널로 알림이 몰릴 때 서킷이 30초 열려 모든 발송이 막힌다 (부하 테스트에서 확인).
     */
    private CircuitBreakerConfig slackCircuitBreakerConfig() {
        return CircuitBreakerConfig.from(circuitBreakerConfig())
                .ignoreExceptions(SlackRateLimitedException.class)
                .build();
    }

    private CircuitBreakerConfig circuitBreakerConfig() {
        return CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10)
                .minimumNumberOfCalls(5)
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(2)
                .build();
    }

    private TimeLimiterConfig timeLimiter(Duration timeout) {
        return TimeLimiterConfig.custom()
                .timeoutDuration(timeout)
                .build();
    }
}
