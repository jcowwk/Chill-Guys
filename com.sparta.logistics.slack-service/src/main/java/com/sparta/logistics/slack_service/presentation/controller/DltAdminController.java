package com.sparta.logistics.slack_service.presentation.controller;

import com.sparta.logistics.slack_service.application.dto.response.DltReplayResponseDto;
import com.sparta.logistics.slack_service.application.service.DltReplayService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * DLT 운영 API (MASTER 전용).
 * Gateway 가 /api/master/** 를 MASTER 로 제한하지만, 서비스에 직접 들어오는 요청에 대비해 여기서도 확인한다.
 *
 *   GET  /api/master/slack/dlt?topic=HUB&max=50          조회 (재투입 대상/제외 사유)
 *   POST /api/master/slack/dlt/replay?topic=HUB&max=50   원인 해결 후 원본 토픽으로 재투입
 */
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/master/slack/dlt")
public class DltAdminController {

    private static final String MASTER = "ROLE_MASTER";

    private final DltReplayService dltReplayService;

    @GetMapping
    public ResponseEntity<?> peek(@RequestHeader(value = "X-User-Role", required = false) String role,
                                  @RequestParam(defaultValue = "HUB") String topic,
                                  @RequestParam(defaultValue = "50") int max) {
        if (!MASTER.equals(role)) return forbidden();
        return ResponseEntity.ok(dltReplayService.peek(topic, max));
    }

    @PostMapping("/replay")
    public ResponseEntity<?> replay(@RequestHeader(value = "X-User-Role", required = false) String role,
                                    @RequestParam(defaultValue = "HUB") String topic,
                                    @RequestParam(defaultValue = "50") int max) {
        if (!MASTER.equals(role)) return forbidden();
        DltReplayResponseDto result = dltReplayService.replay(topic, max);
        return ResponseEntity.ok(result);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("message", e.getMessage()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, String>> conflict(IllegalStateException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("message", e.getMessage()));
    }

    private static ResponseEntity<Map<String, String>> forbidden() {
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("message", "MASTER 권한이 필요합니다."));
    }
}
