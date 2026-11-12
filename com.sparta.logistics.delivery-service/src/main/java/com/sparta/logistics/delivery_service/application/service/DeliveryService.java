package com.sparta.logistics.delivery_service.application.service;

import com.sparta.logistics.delivery_service.application.dto.request.OrderDeliveryRequestDto;
import com.sparta.logistics.delivery_service.application.dto.request.DeliveryUpdateRequestDto;
import com.sparta.logistics.delivery_service.application.dto.response.DeliveryManagerInfoDto;
import com.sparta.logistics.delivery_service.application.dto.response.DeliveryResponseDto;
import com.sparta.logistics.delivery_service.infrastructure.messaging.dto.DeliveryInfoMapper;
import com.sparta.logistics.delivery_service.infrastructure.messaging.outbox.OutboxEventRecorder;
import com.sparta.logistics.delivery_service.application.mapper.DeliveryMapper;
import com.sparta.logistics.delivery_service.domain.model.Delivery;
import com.sparta.logistics.delivery_service.domain.model.DeliveryStatus;
import com.sparta.logistics.delivery_service.domain.repository.DeliveryRepository;
import com.sparta.logistics.delivery_service.exception.CustomException;
import com.sparta.logistics.delivery_service.exception.DeliveryErrorCode;
import com.sparta.logistics.delivery_service.infrastructure.client.CompanyClient;
import com.sparta.logistics.delivery_service.infrastructure.client.DeliveryManagerClient;
import com.sparta.logistics.delivery_service.infrastructure.client.ProductClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final DeliveryRouteService deliveryRouteService;

    private final DeliveryManagerClient deliveryManagerClient;
    private final CompanyClient companyClient;
    private final ProductClient productClient;

    private final OutboxEventRecorder outboxEventRecorder;

    @Transactional
    public UUID createDelivery(OrderDeliveryRequestDto orderDeliveryRequestDto) {
        // 1. 상품을 보고 출발 허브 결정
        UUID productId = orderDeliveryRequestDto.getProductId();
        UUID departureHubId = productClient.getHubIdByProductId(productId);

        // 2. 수령 업체 보고 도착 허브 결정
        UUID destinationHubId = companyClient.getHubIdByCompanyId(orderDeliveryRequestDto.getRequestCompanyId());

        // 3. 배송 저장
        Delivery delivery = DeliveryMapper.toEntity(orderDeliveryRequestDto, departureHubId, destinationHubId);
        deliveryRepository.save(delivery);

        deliveryRouteService.createDeliveryRoutes(delivery);

        return delivery.getId();
    }

    @Transactional(readOnly = true)
    public List<DeliveryResponseDto> getDeliveryList() {
        List<Delivery> deliveryList = deliveryRepository.findByAndDeletedAtIsNull();

        List<DeliveryResponseDto> deliveryResponseDtoList = new ArrayList<>();
        for(Delivery delivery : deliveryList) {
            deliveryResponseDtoList.add(DeliveryMapper.toDto(delivery));
        }
        return deliveryResponseDtoList;
    }

    @Transactional(readOnly = true)
    public DeliveryResponseDto getDelivery(UUID deliveryId) {
        Delivery delivery = findDeliveryById(deliveryId);
        return DeliveryMapper.toDto(delivery);
    }

    @Transactional
    public void updateDelivery(UUID deliveryId, DeliveryUpdateRequestDto deliveryUpdateRequestDto) {
        Delivery delivery = findDeliveryById(deliveryId);

        if(delivery.isInfoChangeable()) {
            delivery.updateOf(deliveryUpdateRequestDto);
        } else throw new CustomException(DeliveryErrorCode.DELIVERY_IN_START);

        deliveryRepository.save(delivery);
    }

    @Transactional
    public void deleteDelivery(UUID deliveryId, String userId) {
        Delivery delivery = findDeliveryById(deliveryId);
        Long id = Long.parseLong(userId);
        delivery.deletedOf(id);
    }

    @Transactional
    public void assignPendingDeliveries() {
        List<Delivery> pendingDeliveryies = deliveryRepository.findByDeliveryStatusAndDeletedAtIsNull(DeliveryStatus.PENDING);

        if(!pendingDeliveryies.isEmpty()) {

            for(Delivery delivery : pendingDeliveryies) {
                UUID departureHubId = delivery.getDepartureHubId();
                UUID destinationHubId = delivery.getDestinationHubId();
                String type = "HUB";
                DeliveryManagerInfoDto dto = deliveryManagerClient.assignDeliveryManager(departureHubId, destinationHubId, type);
                delivery.assignDeliveryManager(dto.getId());

                log.info("DeliveryManager Assigned");

                // 배송정보를 Outbox 에 기록 (배송 상태 변경과 같은 트랜잭션으로 커밋 → Kafka 장애 시에도 유실 없음)
                // 실제 Kafka 발행은 OutboxRelay 가 비동기로 수행
                outboxEventRecorder.record(delivery.getId(), type,
                        DeliveryInfoMapper.toDto(delivery, dto.getSlackId(), delivery.getRecipientCompany().getAddress()));
            }
        }
    }

    @Transactional
    public void changeDeliveryStatus(UUID deliveryId, DeliveryStatus deliveryStatus) {
        Delivery delivery = findDeliveryById(deliveryId);

        delivery.changeDeliveryStatus(deliveryStatus);
    }


    private Delivery findDeliveryById(UUID deliveryId) {
        return deliveryRepository.findByIdAndDeletedAtIsNull(deliveryId)
                .orElseThrow(() -> new CustomException(DeliveryErrorCode.DELIVERY_NOT_FOUND));
    }
}
