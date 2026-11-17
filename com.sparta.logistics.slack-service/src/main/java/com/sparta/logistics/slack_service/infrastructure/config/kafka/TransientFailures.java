package com.sparta.logistics.slack_service.infrastructure.config.kafka;

import com.sparta.logistics.slack_service.infrastructure.client.OrderNotReadyException;
import feign.FeignException;
import feign.RetryableException;

import java.net.ConnectException;
import java.net.SocketTimeoutException;

/**
 * 잠시 기다리면 저절로 풀리는 실패인지 판단한다.
 *
 * 내부 서비스(order, hub, product)가 재시작·배포 중이거나 잠깐 응답하지 못하는 경우,
 * 주문이 아직 처리 중이라 알림을 보낼지 정할 수 없는 경우로, 기본 재시도(수 초)보다 길게 기다렸다가 다시 처리한다.
 */
public final class TransientFailures {

    private TransientFailures() {
    }

    public static boolean isTransient(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof OrderNotReadyException     // 주문 처리 중
                    || t instanceof RetryableException   // 연결 거부, 읽기 timeout
                    || t instanceof ConnectException
                    || t instanceof SocketTimeoutException) {
                return true;
            }
            if (t instanceof FeignException fe && fe.status() >= 500) {   // 503(대상 인스턴스 없음) 등
                return true;
            }
        }
        return false;
    }
}
