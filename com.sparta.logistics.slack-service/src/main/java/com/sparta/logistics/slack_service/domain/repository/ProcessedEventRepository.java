package com.sparta.logistics.slack_service.domain.repository;

import com.sparta.logistics.slack_service.domain.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {

    @Modifying
    @Query("DELETE FROM ProcessedEvent e WHERE e.processedAt < :before")
    int deleteByProcessedAtBefore(@Param("before") LocalDateTime before);
}
