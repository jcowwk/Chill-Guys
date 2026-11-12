package com.sparta.logistics.slack_service.domain.repository;

import com.sparta.logistics.slack_service.domain.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
}
