package com.sparta.logistics.slack_service.application.service;

import com.sparta.logistics.slack_service.application.dto.request.SlackMessageSendRequestDto;
import com.sparta.logistics.slack_service.domain.entity.Slack;
import com.sparta.logistics.slack_service.domain.repository.SlackRepository;
import com.sparta.logistics.slack_service.infrastructure.client.*;
import com.sparta.logistics.slack_service.infrastructure.config.ResilienceConfig;
import com.sparta.logistics.slack_service.infrastructure.dto.DeliveryInfoDto;
import com.sparta.logistics.slack_service.infrastructure.dto.HubResponseDto;
import com.sparta.logistics.slack_service.infrastructure.dto.OrderSlackDto;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JCircuitBreakerFactory;
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JConfigurationProperties;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 외부 API(AI, Slack) 장애가 배송 알림 흐름 전체로 번지지 않는지 검증.
 * 서킷 브레이커는 운영과 동일한 ResilienceConfig 설정을 사용한다.
 */
class SlackServiceResilienceTest {

    private SlackRepository slackRepository;
    private AiClient aiClient;
    private SlackClient slackClient;
    private HubClient hubClient;
    private OrderClient orderClient;
    private ProductClient productClient;

    private final Map<UUID, Slack> store = new HashMap<>();

    @BeforeEach
    void setUp() {
        slackRepository = mock(SlackRepository.class);
        aiClient = mock(AiClient.class);
        slackClient = mock(SlackClient.class);
        hubClient = mock(HubClient.class);
        orderClient = mock(OrderClient.class);
        productClient = mock(ProductClient.class);

        when(slackRepository.save(any(Slack.class))).thenAnswer(inv -> {
            Slack slack = inv.getArgument(0);
            store.put(slack.getId(), slack);
            return slack;
        });
        when(slackRepository.findById(any(UUID.class))).thenAnswer(inv -> Optional.ofNullable(store.get(inv.<UUID>getArgument(0))));

        when(hubClient.getAddressByHubId(any())).thenReturn(HubResponseDto.builder().hubName("서울 허브").address("서울시").build());
        when(orderClient.getOrderInfo(any())).thenReturn(new OrderSlackDto(3L, "빠른 배송"));
        when(productClient.getProductName(any())).thenReturn("상품A");
        when(slackClient.sendSlackMessage(anyString(), any())).thenReturn("{\"ok\":true}");
    }

    @Test
    @DisplayName("AI 서비스 장애 시 대체 문구로 Slack 알림이 정상 발송된다")
    void aiFailure_sendsSlackWithFallbackText() {
        when(aiClient.generateCompletion(anyString())).thenThrow(new RuntimeException("503 Gemini unavailable"));

        slackService(productionFactory()).saveSlackMessage(delivery());

        ArgumentCaptor<SlackMessageSendRequestDto> captor = ArgumentCaptor.forClass(SlackMessageSendRequestDto.class);
        verify(slackClient).sendSlackMessage(anyString(), captor.capture());
        assertThat(captor.getValue().getText()).contains("AI 예상 배송 날짜: " + SlackService.AI_FALLBACK_TEXT);
    }

    @Test
    @DisplayName("AI 장애가 지속되면 서킷이 열려 AI 호출을 중단하고, 알림은 계속 발송된다")
    void aiDown_circuitOpens_stopsCallingAi() {
        when(aiClient.generateCompletion(anyString())).thenThrow(new RuntimeException("503 Gemini unavailable"));
        SlackService slackService = slackService(productionFactory());

        for (int i = 0; i < 20; i++) {
            slackService.saveSlackMessage(delivery());
        }

        // minimumNumberOfCalls(5) 만큼 실패한 뒤 OPEN → 이후 15건은 AI 를 호출하지 않음
        verify(aiClient, times(5)).generateCompletion(anyString());
        verify(slackClient, times(20)).sendSlackMessage(anyString(), any());
    }

    @Test
    @DisplayName("AI 응답이 시간 제한을 넘기면 기다리지 않고 대체 문구로 발송한다")
    void aiSlow_timeLimiterFallsBack() {
        when(aiClient.generateCompletion(anyString())).thenAnswer(inv -> {
            Thread.sleep(3_000);
            return "2025-03-28에 도착할 예정입니다.";
        });
        Resilience4JCircuitBreakerFactory factory = newFactory();
        factory.configure(builder -> builder
                .circuitBreakerConfig(CircuitBreakerConfig.ofDefaults())
                .timeLimiterConfig(TimeLimiterConfig.custom().timeoutDuration(Duration.ofMillis(300)).build()), ResilienceConfig.AI);

        long start = System.nanoTime();
        slackService(factory).saveSlackMessage(delivery());
        long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();

        ArgumentCaptor<SlackMessageSendRequestDto> captor = ArgumentCaptor.forClass(SlackMessageSendRequestDto.class);
        verify(slackClient).sendSlackMessage(anyString(), captor.capture());
        assertThat(captor.getValue().getText()).contains(SlackService.AI_FALLBACK_TEXT);
        assertThat(elapsedMs).isLessThan(1_500);
    }

    @Test
    @DisplayName("Slack API 장애 시에도 예외가 전파되지 않고 메시지는 저장된다")
    void slackFailure_doesNotPropagate() {
        when(aiClient.generateCompletion(anyString())).thenReturn("2025-03-28에 도착할 예정입니다.");
        when(slackClient.sendSlackMessage(anyString(), any())).thenThrow(new RuntimeException("Slack 5xx"));

        assertThatCode(() -> slackService(productionFactory()).saveSlackMessage(delivery()))
                .doesNotThrowAnyException();
        verify(slackRepository).save(any(Slack.class));
    }

    private SlackService slackService(Resilience4JCircuitBreakerFactory factory) {
        SlackService slackService = new SlackService(slackRepository, aiClient, slackClient, hubClient, orderClient, productClient, factory);
        ReflectionTestUtils.setField(slackService, "SLACK_BOT_TOKEN", "test-token");
        return slackService;
    }

    private Resilience4JCircuitBreakerFactory productionFactory() {
        Resilience4JCircuitBreakerFactory factory = newFactory();
        new ResilienceConfig().externalApiCircuitBreakerCustomizer().customize(factory);
        return factory;
    }

    private Resilience4JCircuitBreakerFactory newFactory() {
        return new Resilience4JCircuitBreakerFactory(
                CircuitBreakerRegistry.ofDefaults(), TimeLimiterRegistry.ofDefaults(), null, new Resilience4JConfigurationProperties());
    }

    private DeliveryInfoDto delivery() {
        return DeliveryInfoDto.builder()
                .departureHubId(UUID.randomUUID())
                .destinationHubId(UUID.randomUUID())
                .orderId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .slackId("C0123456")
                .address("서울시 강남구")
                .build();
    }
}
