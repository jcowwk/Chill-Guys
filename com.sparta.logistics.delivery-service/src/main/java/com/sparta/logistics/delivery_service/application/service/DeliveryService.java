package com.sparta.logistics.delivery_service.application.service;

import com.sparta.logistics.delivery_service.application.dto.request.OrderDeliveryRequestDto;
import com.sparta.logistics.delivery_service.application.dto.request.DeliveryUpdateRequestDto;
import com.sparta.logistics.delivery_service.application.dto.response.DeliveryResponseDto;
import com.sparta.logistics.delivery_service.application.mapper.DeliveryMapper;
import com.sparta.logistics.delivery_service.domain.model.Delivery;
import com.sparta.logistics.delivery_service.domain.model.DeliveryStatus;
import com.sparta.logistics.delivery_service.domain.repository.DeliveryRepository;
import com.sparta.logistics.delivery_service.exception.CustomException;
import com.sparta.logistics.delivery_service.exception.DeliveryErrorCode;
import com.sparta.logistics.delivery_service.infrastructure.client.CompanyClient;
import com.sparta.logistics.delivery_service.infrastructure.client.ProductClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import com.sparta.logistics.delivery_service.infrastructure.monitoring.DeliveryPipelineMetrics;
import java.util.concurrent.ExecutorService;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final DeliveryRouteService deliveryRouteService;

    private final CompanyClient companyClient;
    private final ProductClient productClient;

    private final DeliveryAssignmentProcessor deliveryAssignmentProcessor;
    private final ExecutorService deliveryAssignmentExecutor;
    private final DeliveryPipelineMetrics pipelineMetrics;

    @Value("${delivery.assignment.batch-size:200}")
    private int assignmentBatchSize;

    @Value("${delivery.assignment.max-run-ms:50000}")
    private long assignmentMaxRunMs;

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

    /**
     * 배정 대기 배송을 건별 트랜잭션으로 처리한다 (트랜잭션 경계는 DeliveryAssignmentProcessor).
     * 이 메서드 자체에는 트랜잭션을 걸지 않는다.
     */
    public PendingAssignmentLoop.Result assignPendingDeliveries() {
        PendingAssignmentLoop.Result result = PendingAssignmentLoop.run("DeliveryAssign", assignmentBatchSize, assignmentMaxRunMs,
                deliveryAssignmentExecutor,
                deliveryRepository::findPendingRefsAfter, deliveryAssignmentProcessor::assignDelivery);
        pipelineMetrics.assignmentResult(result.assigned(), result.failed());
        return result;
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
