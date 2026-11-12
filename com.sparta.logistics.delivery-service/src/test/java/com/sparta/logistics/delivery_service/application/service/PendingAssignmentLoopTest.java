package com.sparta.logistics.delivery_service.application.service;

import com.sparta.logistics.delivery_service.domain.repository.PendingRef;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

class PendingAssignmentLoopTest {

    record Ref(UUID id, LocalDateTime at) implements PendingRef {
        public UUID getId() { return id; }
        public LocalDateTime getCreatedAt() { return at; }
    }

    /** 커서 이후의 대기 건만 돌려주는 가짜 저장소 (배정에 성공한 건은 대기 목록에서 빠짐) */
    static class FakeStore {
        final List<Ref> pending = new ArrayList<>();
        int fetches = 0;

        FakeStore(int n) {
            LocalDateTime base = LocalDateTime.of(2026, 1, 1, 0, 0);
            for (int i = 0; i < n; i++) pending.add(new Ref(new UUID(0, i + 1), base.plusSeconds(i)));
        }

        List<PendingRef> fetch(LocalDateTime at, UUID id, int limit) {
            fetches++;
            return pending.stream()
                    .filter(r -> r.at.isAfter(at) || (r.at.isEqual(at) && r.id.compareTo(id) > 0))
                    .limit(limit).map(r -> (PendingRef) r).toList();
        }
    }

    @Test
    void 한_건이_실패해도_나머지는_배정하고_실패_건은_같은_실행에서_다시_집지_않는다() {
        FakeStore store = new FakeStore(5);
        UUID failing = store.pending.get(1).id;
        Map<UUID, Integer> attempts = new HashMap<>();

        PendingAssignmentLoop.Result r = PendingAssignmentLoop.run("t", 2, 10_000, store::fetch, id -> {
            attempts.merge(id, 1, Integer::sum);
            if (id.equals(failing)) throw new IllegalStateException("delivery-manager 오류");
            store.pending.removeIf(x -> x.id.equals(id));
            return true;
        });

        assertThat(r.assigned()).isEqualTo(4);
        assertThat(r.failed()).isEqualTo(1);
        assertThat(attempts.get(failing)).isEqualTo(1);
        assertThat(store.pending).extracting(Ref::id).containsExactly(failing);
    }

    @Test
    void 배치_전체가_실패하면_하위_서비스_장애로_보고_중단한다() {
        FakeStore store = new FakeStore(10);
        int[] calls = {0};

        PendingAssignmentLoop.Result r = PendingAssignmentLoop.run("t", 3, 10_000, store::fetch, id -> {
            calls[0]++;
            throw new IllegalStateException("connection refused");
        });

        assertThat(r.stoppedByFailure()).isTrue();
        assertThat(calls[0]).isEqualTo(3);
    }

    @Test
    void 이미_처리된_건은_건너뜀으로_집계한다() {
        FakeStore store = new FakeStore(3);

        PendingAssignmentLoop.Result r = PendingAssignmentLoop.run("t", 10, 10_000, store::fetch, id -> false);

        assertThat(r.skipped()).isEqualTo(3);
        assertThat(r.assigned()).isZero();
    }

    @Test
    void 동시_처리해도_모든_건을_한_번씩_처리하고_실패는_격리된다() {
        FakeStore store = new FakeStore(50);
        List<Ref> snapshot = List.copyOf(store.pending);
        UUID failing = snapshot.get(7).id;
        Map<UUID, Integer> attempts = new ConcurrentHashMap<>();
        Set<String> threads = ConcurrentHashMap.newKeySet();
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            PendingAssignmentLoop.Result r = PendingAssignmentLoop.run("t", 10, 10_000, pool,
                    // 가짜 저장소는 단일 스레드 접근만 하도록 커서 기반 스냅샷 조회
                    (at, id, limit) -> snapshot.stream()
                            .filter(x -> x.at.isAfter(at) || (x.at.isEqual(at) && x.id.compareTo(id) > 0))
                            .limit(limit).map(x -> (PendingRef) x).toList(),
                    id -> {
                        attempts.merge(id, 1, Integer::sum);
                        threads.add(Thread.currentThread().getName());
                        if (id.equals(failing)) throw new IllegalStateException("boom");
                        return true;
                    });

            assertThat(r.assigned()).isEqualTo(49);
            assertThat(r.failed()).isEqualTo(1);
            assertThat(attempts).hasSize(50).allSatisfy((id, n) -> assertThat(n).isEqualTo(1));
            assertThat(threads.size()).isGreaterThan(1);
        } finally {
            pool.shutdownNow();
        }
    }
}
