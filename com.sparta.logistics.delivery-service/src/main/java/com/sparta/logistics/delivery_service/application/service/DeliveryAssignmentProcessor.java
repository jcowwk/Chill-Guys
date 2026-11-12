package com.sparta.logistics.delivery_service.application.service;

import com.sparta.logistics.delivery_service.application.dto.response.DeliveryManagerInfoDto;
import com.sparta.logistics.delivery_service.domain.model.Delivery;
import com.sparta.logistics.delivery_service.domain.model.DeliveryRoute;
import com.sparta.logistics.delivery_service.domain.repository.DeliveryRepository;
import com.sparta.logistics.delivery_service.domain.repository.DeliveryRouteRepository;
import com.sparta.logistics.delivery_service.infrastructure.client.DeliveryManagerClient;
import com.sparta.logistics.delivery_service.infrastructure.messaging.dto.DeliveryInfoMapper;
import com.sparta.logistics.delivery_service.infrastructure.messaging.outbox.OutboxEventRecorder;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

/**
 * 배송/배송경로 한 건의 담당자 배정을 독립된 트랜잭션으로 처리한다.
 *
 * 기존에는 대기 건 전체(부하 테스트에서 한 번에 1만 건 이상)를 한 트랜잭션에서 처리해서
 * - 한 건만 실패해도 전체가 롤백되고 (원격에서 이미 배정된 담당자는 그대로 남음)
 * - 트랜잭션이 최대 60초 동안 열려 있었고
 * - 인스턴스가 여러 대면 같은 배송을 중복 배정할 수 있었다.
 *
 * 루프(서비스)와 분리된 빈인 이유: REQUIRES_NEW 는 프록시를 거쳐야 적용된다 (self-invocation 불가).
 */
@Component
@RequiredArgsConstructor
public class DeliveryAssignmentProcessor {

    static final String HUB_TYPE = "HUB";
    static final String COMPANY_TYPE = "COMPANY";

    private final DeliveryRepository deliveryRepository;
    private final DeliveryRouteRepository deliveryRouteRepository;
    private final DeliveryManagerClient deliveryManagerClient;
    private final OutboxEventRecorder outboxEventRecorder;

    /** @return true = 배정함, false = 이미 배정됐거나 다른 인스턴스가 처리 중 */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean assignDelivery(UUID deliveryId) {
        Optional<Delivery> locked = deliveryRepository.findPendingByIdForUpdateSkipLocked(deliveryId);
        if (locked.isEmpty()) return false;
        Delivery delivery = locked.get();

        DeliveryManagerInfoDto manager = deliveryManagerClient.assignDeliveryManager(
                delivery.getDepartureHubId(), delivery.getDestinationHubId(), HUB_TYPE);
        delivery.assignDeliveryManager(manager.getId());

        // 상태 변경과 같은 트랜잭션으로 Outbox 기록 → 커밋되면 알림 이벤트도 보장
        outboxEventRecorder.record(delivery.getId(), HUB_TYPE,
                DeliveryInfoMapper.toDto(delivery, manager.getSlackId(), delivery.getRecipientCompany().getAddress()));
        return true;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean assignRoute(UUID routeId) {
        Optional<DeliveryRoute> locked = deliveryRouteRepository.findPendingByIdForUpdateSkipLocked(routeId);
        if (locked.isEmpty()) return false;
        DeliveryRoute route = locked.get();

        DeliveryManagerInfoDto manager = deliveryManagerClient.assignDeliveryManager(
                route.getStartHudId(), route.getEndHudId(), COMPANY_TYPE);
        route.assignHubDeliveryManager(manager.getId());
        return true;
    }
}
