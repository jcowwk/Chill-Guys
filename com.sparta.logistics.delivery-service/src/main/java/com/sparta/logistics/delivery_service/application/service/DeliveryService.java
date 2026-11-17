package com.sparta.logistics.delivery_service.application.service;

import com.sparta.logistics.delivery_service.application.dto.request.OrderDeliveryRequestDto;
import com.sparta.logistics.delivery_service.application.dto.request.DeliveryUpdateRequestDto;
import com.sparta.logistics.delivery_service.application.dto.response.DeliveryResponseDto;
import com.sparta.logistics.delivery_service.application.mapper.DeliveryMapper;
import com.sparta.logistics.delivery_service.domain.model.Delivery;
import com.sparta.logistics.delivery_service.domain.model.DeliveryCancellation;
import com.sparta.logistics.delivery_service.domain.model.DeliveryStatus;
import com.sparta.logistics.delivery_service.domain.repository.DeliveryCancellationRepository;
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

import java.time.LocalDateTime;
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
    private final DeliveryCancellationRepository deliveryCancellationRepository;

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

        // 3. 같은 주문의 취소와 순서를 맞춘다: 취소가 먼저 끝났으면 만들지 않는다
        //    (주문 쪽이 생성 응답을 기다리다 포기하고 바로 취소한 경우, 이 생성이 취소보다 늦게 커밋되면 배송이 남는다)
        UUID orderId = orderDeliveryRequestDto.getOrderId();
        if (orderId != null) {
            deliveryCancellationRepository.lockOrder(orderId);
            if (deliveryCancellationRepository.existsById(orderId)) {
                log.warn("취소된 주문의 늦은 배송 생성 거절 orderId={}", orderId);
                throw new CustomException(DeliveryErrorCode.ORDER_CANCELLED);
            }
        }

        // 4. 배송 저장
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

    /**
     * 주문 생성이 실패해 보상할 때 그 주문의 배송을 취소한다 (배송·경로를 소프트 삭제 → 배정 대상에서 빠짐).
     * 배송 생성 응답만 끊기고 실제로는 만들어진 경우를 정리하기 위함이다. 없으면 아무것도 하지 않는다 (여러 번 호출해도 안전).
     * 취소 표시를 남겨, 이 취소보다 늦게 도착한 같은 주문의 배송 생성은 거절된다.
     * 이미 배정되어 알림 이벤트가 나간 경우는 알림 서비스가 주문 상태를 확인해 걸러낸다.
     *
     * @return 취소한 배송 수
     */
    @Transactional
    public int cancelByOrderId(UUID orderId) {
        // 생성 중인 같은 주문의 배송이 있으면 그 커밋을 기다린 뒤 취소한다
        deliveryCancellationRepository.lockOrder(orderId);
        if (!deliveryCancellationRepository.existsById(orderId)) {
            deliveryCancellationRepository.save(new DeliveryCancellation(orderId, LocalDateTime.now()));
        }
        List<Delivery> deliveries = deliveryRepository.findByOrderIdAndDeletedAtIsNull(orderId);
        for (Delivery delivery : deliveries) {
            if (delivery.getDeliveryStatus() != DeliveryStatus.PENDING) {
                log.warn("이미 배정된 배송 취소 deliveryId={}, status={} - 알림은 알림 서비스에서 주문 상태로 거름",
                        delivery.getId(), delivery.getDeliveryStatus());
            }
            delivery.deletedOf(null);
            deliveryRouteService.cancelRoutes(delivery.getId());
        }
        return deliveries.size();
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
