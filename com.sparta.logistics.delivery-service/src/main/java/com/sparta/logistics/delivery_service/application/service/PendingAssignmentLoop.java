package com.sparta.logistics.delivery_service.application.service;

import com.sparta.logistics.delivery_service.domain.repository.PendingRef;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

/**
 * 배정 대기 건을 작은 배치로 나눠 한 건씩(건별 트랜잭션) 처리하는 루프.
 *
 * - (생성 시각, ID) 커서로 앞으로만 진행: 실패한 건은 이번 실행에서 다시 집지 않고 다음 실행에서 재시도
 * - 배치 전체가 실패하면 하위 서비스 장애로 보고 중단 (장애 중 수천 건을 계속 두드리지 않음)
 * - 최대 실행 시간을 넘기면 중단 (다음 스케줄과 겹치지 않게)
 * - 배치 안의 건들은 executor 로 동시에 처리 (건별 커밋 비용 때문에 순차 처리 시 처리량이 1/3 로 떨어짐 - 부하 테스트)
 *   같은 건을 두 스레드가 잡지 않는 것은 SELECT ... FOR UPDATE SKIP LOCKED 가 보장한다.
 */
@Slf4j
public final class PendingAssignmentLoop {

    public record Result(int assigned, int skipped, int failed, boolean stoppedByFailure) {
    }

    @FunctionalInterface
    public interface Fetcher {
        List<PendingRef> fetch(LocalDateTime afterCreatedAt, UUID afterId, int limit);
    }

    private static final LocalDateTime START_AT = LocalDateTime.of(1970, 1, 1, 0, 0);
    private static final UUID START_ID = new UUID(0L, 0L);

    private PendingAssignmentLoop() {
    }

    /** 순차 처리 */
    public static Result run(String name, int batchSize, long maxRunMs, Fetcher fetcher, Predicate<UUID> assignOne) {
        return run(name, batchSize, maxRunMs, Runnable::run, fetcher, assignOne);
    }

    /**
     * @param assignOne 한 건 배정. true = 배정함, false = 이미 처리됐거나 다른 인스턴스가 처리 중(건너뜀), 예외 = 실패
     */
    public static Result run(String name, int batchSize, long maxRunMs, Executor executor,
                             Fetcher fetcher, Predicate<UUID> assignOne) {
        long deadline = System.currentTimeMillis() + maxRunMs;
        LocalDateTime cursorAt = START_AT;
        UUID cursorId = START_ID;
        int assigned = 0, skipped = 0, failed = 0;

        while (System.currentTimeMillis() < deadline) {
            List<PendingRef> batch = fetcher.fetch(cursorAt, cursorId, batchSize);
            if (batch.isEmpty()) break;

            List<CompletableFuture<Boolean>> futures = new ArrayList<>(batch.size());
            for (PendingRef ref : batch) {
                futures.add(CompletableFuture.supplyAsync(() -> assignOne.test(ref.getId()), executor));
            }

            int batchFailed = 0;
            for (int i = 0; i < batch.size(); i++) {
                try {
                    if (futures.get(i).join()) assigned++;
                    else skipped++;
                } catch (CompletionException e) {
                    failed++;
                    batchFailed++;
                    Throwable cause = e.getCause() != null ? e.getCause() : e;
                    log.warn("[{}] 배정 실패 id={}, 다음 실행에서 재시도: {}", name, batch.get(i).getId(), cause.toString());
                }
            }
            PendingRef last = batch.get(batch.size() - 1);
            cursorAt = last.getCreatedAt();
            cursorId = last.getId();

            if (batchFailed == batch.size()) {
                log.error("[{}] 배치 {}건 전부 실패 - 하위 서비스 장애로 판단하고 이번 실행 중단", name, batch.size());
                return new Result(assigned, skipped, failed, true);
            }
            if (batch.size() < batchSize) break;
        }
        if (assigned + failed > 0) {
            log.info("[{}] 배정 {}건, 건너뜀 {}건, 실패 {}건", name, assigned, skipped, failed);
        }
        return new Result(assigned, skipped, failed, false);
    }
}
