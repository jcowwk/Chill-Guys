package com.sparta.logistics.slack_service.application.service;

import com.sparta.logistics.slack_service.application.dto.response.SlackMessageSaveResponseDto;
import com.sparta.logistics.slack_service.domain.entity.Slack;
import com.sparta.logistics.slack_service.infrastructure.client.*;
import com.sparta.logistics.slack_service.infrastructure.config.ResilienceConfig;
import com.sparta.logistics.slack_service.infrastructure.dto.DeliveryInfoDto;
import com.sparta.logistics.slack_service.infrastructure.dto.HubResponseDto;
import com.sparta.logistics.slack_service.domain.repository.SlackRepository;
import com.sparta.logistics.slack_service.infrastructure.dto.OrderSlackDto;
import com.sparta.logistics.slack_service.infrastructure.monitoring.NotificationMetrics;
import com.sparta.logistics.slack_service.infrastructure.slack.SlackSender;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.client.circuitbreaker.CircuitBreakerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
public class SlackService {

    // AI 장애 시에도 배송 알림은 나가도록 사용하는 대체 문구
    static final String AI_FALLBACK_TEXT = "AI 예측 불가 (담당자 확인 필요)";

    /** 알림을 보내지 않는 주문 상태 */
    private static final java.util.Set<String> SKIPPED_ORDER_STATUSES = java.util.Set.of("FAILED", "CANCELLED");

    private final SlackRepository slackRepository;
    private final AiClient aiClient;
    private final HubClient hubClient;
    private final OrderClient orderClient;
    private final ProductClient productClient;
    private final CircuitBreakerFactory<?, ?> circuitBreakerFactory;
    private final SlackSender slackSender;
    private NotificationMetrics metrics;

    public SlackService(SlackRepository slackRepository, AiClient aiClient, HubClient hubClient, OrderClient orderClient, ProductClient productClient, CircuitBreakerFactory<?, ?> circuitBreakerFactory, SlackSender slackSender) {
        this.slackRepository = slackRepository;
        this.aiClient = aiClient;
        this.hubClient = hubClient;
        this.orderClient = orderClient;
        this.productClient = productClient;
        this.circuitBreakerFactory = circuitBreakerFactory;
        this.slackSender = slackSender;
    }

    @Autowired(required = false)
    void setMetrics(NotificationMetrics metrics) {
        this.metrics = metrics;
    }

    /**
     * HTTP API(/api/slack/save) 용: 알림을 만들어 저장하고 발송한다.
     * 발송 실패는 응답을 막지 않도록 로그로만 남긴다 (기존 동작 유지).
     */
    public SlackMessageSaveResponseDto saveSlackMessage(DeliveryInfoDto deliveryResponseDto) {
        Slack slack = saveNew(deliveryResponseDto, null, orderClient.getOrderInfo(deliveryResponseDto.getOrderId()));
        sendSlackMessage(slack.getId(), slack.getSlackId());
        return SlackMessageSaveResponseDto.builder()
                .id(slack.getId())
                .slackId(slack.getSlackId())
                .text(slack.getText())
                .build();
    }

    /**
     * Kafka 알림 처리: 실패하면 예외를 던져 컨슈머가 재시도하거나 DLT 로 보내게 한다.
     *
     * - 같은 eventId 로 이미 만든 알림이 있으면 다시 만들지 않는다 (재시도 때 AI 재호출, 중복 저장 방지)
     * - 이미 발송된 알림이면 다시 보내지 않는다
     */
    public void processNotification(DeliveryInfoDto delivery, String eventId) {
        // 주문 생성이 실패(보상 완료)했거나 취소된 주문이면 배송 알림을 보내지 않는다.
        // 배송 생성 응답만 끊겨 배송이 남은 경우, 배송 취소보다 배정·알림 이벤트가 먼저 나간 경우를 여기서 막는다.
        OrderSlackDto order = orderClient.getOrderInfo(delivery.getOrderId());
        if (order != null && SKIPPED_ORDER_STATUSES.contains(order.getStatus())) {
            log.warn("실패·취소된 주문의 배송 알림 생략 orderId={}, status={}", delivery.getOrderId(), order.getStatus());
            return;
        }
        Slack slack = (eventId == null ? Optional.<Slack>empty() : slackRepository.findByEventId(eventId))
                .orElseGet(() -> saveNew(delivery, eventId, order));
        if (slack.getSentAt() != null) {
            return;
        }
        slackSender.send(slack.getId(), slack.getSlackId(), slack.getText());
        slack.markSent(new Timestamp(System.currentTimeMillis()));
        slackRepository.save(slack);
    }

    private Slack saveNew(DeliveryInfoDto delivery, String eventId, OrderSlackDto order) {
        Slack slack = Slack.builder()
                .id(UUID.randomUUID())
                .slackId(delivery.getSlackId())
                .text(buildSlackText(delivery, order))
                .createdAt(new Timestamp(System.currentTimeMillis()))
                .eventId(eventId)
                .build();
        return slackRepository.save(slack);
    }

    /** 허브·주문·상품·AI 정보를 모아 알림 문구를 만든다 (외부 API 호출 포함) */
    private String buildSlackText(DeliveryInfoDto deliveryResponseDto, OrderSlackDto orderSlackDto) {

        // 1. hubId로 hub 주소 찾기
        HubResponseDto hubStartResponseDto = hubClient.getAddressByHubId(deliveryResponseDto.getDepartureHubId());
        HubResponseDto hubEndResponseDto = hubClient.getAddressByHubId(deliveryResponseDto.getDestinationHubId());

        // 2. delivery 정보 + hub 주소를 AI 요청용 string으로 변환
        String inputForAI = convertDeliveryInfoToString(deliveryResponseDto, hubStartResponseDto, hubEndResponseDto);

        // 3. string 으로 AI api에 전달하여 최종 발송 시한 응답값 받기
        //    AI(Gemini) 지연/장애가 알림 발송 실패로 번지지 않도록 서킷 브레이커 + fallback 으로 격리
        String output = circuitBreakerFactory.create(ResilienceConfig.AI).run(
                () -> aiClient.generateCompletion(inputForAI),
                throwable -> {
                    log.warn("AI 배송일 예측 실패, 대체 문구로 발송: {}", throwable.toString());
                    if (metrics != null) metrics.aiFallback();
                    return AI_FALLBACK_TEXT;
                });

        // 주문 상품 정보 추가

        // 주문 정보(수량, 주문 번호)
        UUID orderId = deliveryResponseDto.getOrderId();
        Long quantity = orderSlackDto.getQuantity();
        String reqeust = orderSlackDto.getRequest();

        // 상품 정보(상품 아이디, 이름)
        UUID productId = deliveryResponseDto.getProductId();
        String productName = productClient.getProductName(productId);

        // 4. Slack message string 생성 (AI 응답 포함해서)
        String slackText = buildSlackText(deliveryResponseDto, hubStartResponseDto, hubEndResponseDto, output, orderId, quantity, productId, productName);

        return slackText;
    }

    private String convertDeliveryInfoToString(DeliveryInfoDto deliveryDto, HubResponseDto hubStartResponseDto, HubResponseDto hubEndResponseDto) {
        StringBuilder sb = new StringBuilder();

        sb.append("배송지: ").append(deliveryDto.getAddress()).append("\n");
        sb.append("출발 허브 이름: ").append(hubStartResponseDto.getHubName()).append("\n");
        sb.append("출발 허브 주소: ").append(hubStartResponseDto.getAddress()).append("\n");
        sb.append("도착 허브 이름: ").append(hubEndResponseDto.getHubName()).append("\n");
        sb.append("도착 허브 주소: ").append(hubEndResponseDto.getAddress()).append("\n");

        return sb.toString();
    }

    private String buildSlackText(DeliveryInfoDto deliveryResponseDto, HubResponseDto hubStartDto, HubResponseDto hubEndDto, String output, UUID orderId, Long quantity, UUID productId, String productName) {
        StringBuilder sb = new StringBuilder();

        sb.append("배송지: ").append(deliveryResponseDto.getAddress()).append("\n");
        sb.append("출발 허브 이름: ").append(hubStartDto.getHubName()).append("\n");
        sb.append("출발 허브 주소: ").append(hubStartDto.getAddress()).append("\n");
        sb.append("도착 허브 이름: ").append(hubEndDto.getHubName()).append("\n");
        sb.append("도착 허브 주소: ").append(hubEndDto.getAddress()).append("\n");
        sb.append("주문 ID: ").append(orderId).append("\n");
        sb.append("상품 ID: ").append(productId).append("\n");
        sb.append("상품명: ").append(productName).append("\n");
        sb.append("수량: ").append(quantity).append("\n");
        sb.append("AI 예상 배송 날짜: ").append(output).append("\n");

        return sb.toString();
    }

    /** HTTP API(/api/slack/send) 용: 저장된 알림을 발송한다. 실패는 로그로만 남긴다 (기존 동작 유지). */
    public void sendSlackMessage(UUID id, String slackId) {
        Slack slack = slackRepository.findById(id).orElse(null);
        if (slack == null) {
            log.info("슬랙 데이터 없음");
            return;
        }
        try {
            slackSender.send(slack.getId(), slackId, slack.getText());
            slack.markSent(new Timestamp(System.currentTimeMillis()));
            slackRepository.save(slack);
            log.debug("Slack 메시지 발송 id={}", id);
        } catch (Exception e) {
            log.error("Slack 메시지 발송 실패 id={}: {}", id, e.toString());
        }
    }
}