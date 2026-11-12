package com.sparta.logistics.delivery_service.application.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
public class DeliveryAssignmentExecutorConfig {

    /**
     * 배정 동시 처리 스레드. 각 스레드가 DB 커넥션 1개 + delivery-manager-service 호출 1개를 쓰므로
     * Hikari 풀(기본 10)에서 HTTP 요청 처리 몫을 남겨 두도록 작게 잡는다.
     */
    @Bean(destroyMethod = "shutdown")
    public ExecutorService deliveryAssignmentExecutor(@Value("${delivery.assignment.concurrency:4}") int concurrency) {
        AtomicInteger seq = new AtomicInteger();
        return Executors.newFixedThreadPool(concurrency, r -> {
            Thread t = new Thread(r, "delivery-assign-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
    }
}
