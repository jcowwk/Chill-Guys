package com.sparta.logistics.slack_service.load;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * slack-service 가 호출하는 외부 API(hub/order/product/ai-service, Slack Web API)를 흉내 내는 스텁 서버.
 * AI(Gemini) 응답 지연/장애를 주입해 외부 API 가 느려질 때의 영향을 측정한다.
 */
class ExternalApiStubServer {

    private static final Pattern ORDER_ID = Pattern.compile("주문 ID: ([0-9a-f-]{36})");

    private final HttpServer server;

    volatile long aiDelayMs = 0;
    volatile boolean aiFailing = false;

    final AtomicInteger aiCalls = new AtomicInteger();
    /** orderId → Slack 메시지 수신 시각(ms) */
    final Map<UUID, Long> slackReceivedAt = new ConcurrentHashMap<>();
    final AtomicInteger slackMessages = new AtomicInteger();

    ExternalApiStubServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());

        server.createContext("/api/hubs/", ex -> respond(ex, 200, "application/json",
                "{\"id\":\"" + UUID.randomUUID() + "\",\"userId\":1,\"hubName\":\"서울 허브\",\"address\":\"서울시 송파구\",\"latitude\":37.5,\"longitude\":127.1}"));
        server.createContext("/api/orders/", ex -> respond(ex, 200, "application/json",
                "{\"quantity\":3,\"request\":\"빠른 배송\"}"));
        server.createContext("/api/products/", ex -> respond(ex, 200, "text/plain;charset=UTF-8", "상품A"));
        server.createContext("/api/ai/generate", ex -> {
            aiCalls.incrementAndGet();
            ex.getRequestBody().readAllBytes();
            sleep(aiDelayMs);
            if (aiFailing) {
                respond(ex, 500, "application/json", "{\"message\":\"Gemini unavailable\"}");
            } else {
                respond(ex, 200, "text/plain;charset=UTF-8", "2026-11-14에 도착할 예정입니다.");
            }
        });
        server.createContext("/chat.postMessage", ex -> {
            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            Matcher m = ORDER_ID.matcher(body);
            if (m.find()) {
                slackReceivedAt.putIfAbsent(UUID.fromString(m.group(1)), System.currentTimeMillis());
            }
            slackMessages.incrementAndGet();
            respond(ex, 200, "application/json", "{\"ok\":true}");
        });
    }

    void start() {
        server.start();
    }

    void stop() {
        server.stop(0);
    }

    String baseUrl() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    void reset() {
        aiDelayMs = 0;
        aiFailing = false;
        aiCalls.set(0);
        slackReceivedAt.clear();
        slackMessages.set(0);
    }

    private static void respond(HttpExchange ex, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", contentType);
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static void sleep(long ms) {
        if (ms <= 0) return;
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
