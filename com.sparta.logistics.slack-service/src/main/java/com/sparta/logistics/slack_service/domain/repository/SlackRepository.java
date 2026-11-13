package com.sparta.logistics.slack_service.domain.repository;

import com.sparta.logistics.slack_service.domain.entity.Slack;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface SlackRepository extends JpaRepository<Slack, UUID> {

    Optional<Slack> findByEventId(String eventId);
}
