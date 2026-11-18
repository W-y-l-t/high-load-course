package ru.quipy.payments.logic;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import ru.quipy.core.EventSourcingService;
import ru.quipy.payments.api.PaymentAggregate;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class PaymentTimeoutRetryTest {
    private record Reply(int status, boolean success, String message, long delayMillis) {}

    private HttpServer server;
    private ExecutorService executor;
    private SimpleMeterRegistry registry;
    private final List<Map<String, String>> requests = new CopyOnWriteArrayList<>();
    private final AtomicInteger calls = new AtomicInteger();
    private volatile IntFunction<Reply> replies;
    private final CountDownLatch lateResponseFinished = new CountDownLatch(1);

    @BeforeEach
    void startServer() throws IOException {
        registry = new SimpleMeterRegistry();
        replies = index -> new Reply(200, true, "", 0);
        executor = Executors.newCachedThreadPool();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(executor);
        server.createContext("/external/process", this::handle);
        server.start();
    }

    private void handle(HttpExchange exchange) throws IOException {
        Map<String, String> query = Stream.of(exchange.getRequestURI().getRawQuery().split("&"))
            .map(part -> part.split("=", 2))
            .collect(Collectors.toMap(parts -> parts[0], parts -> URLDecoder.decode(parts[1], StandardCharsets.UTF_8)));
        requests.add(query);
        Reply reply = replies.apply(calls.incrementAndGet());
        try {
            Thread.sleep(reply.delayMillis());
            String body = String.format(
                "{\"transactionId\":\"%s\",\"paymentId\":\"%s\",\"result\":%s,\"message\":\"%s\"}",
                query.get("transactionId"), query.get("paymentId"), reply.success(), reply.message());
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(reply.status(), bytes.length);
            exchange.getResponseBody().write(bytes);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            exchange.close();
            if (reply.delayMillis() > 1500) lateResponseFinished.countDown();
        }
    }

    @SuppressWarnings("unchecked")
    private PaymentExternalSystemAdapterImpl adapter(int rate) {
        // The library has Kotlin overloads distinguished by their JVM return type.
        // Return null explicitly instead of Mockito's default empty List answer.
        EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState> events =
            mock(EventSourcingService.class, invocation -> null);
        var properties = new PaymentAccountProperties("test", "acc-7", 50, rate, 30, Duration.ofMillis(1200), true);
        return new PaymentExternalSystemAdapterImpl(properties, events,
            "127.0.0.1:" + server.getAddress().getPort(), "test-token", registry);
    }

    private void pay(PaymentExternalSystemAdapterImpl adapter, long budgetMillis) {
        long start = System.currentTimeMillis();
        adapter.performPaymentAsync(UUID.randomUUID(), 100, start, start + budgetMillis);
    }

    private double retries() {
        return registry.get("payments.provider.retries").counter().count();
    }

    private long outcomes(String outcome) {
        return registry.get("payments.provider.request.duration").tag("outcome", outcome).timer().count();
    }

    @Test
    void retriesServerTimeoutsAndPreservesPaymentIdentity() {
        replies = n -> n < 3 ? new Reply(408, false, "Timeout", 0) : new Reply(200, true, "", 0);
        pay(adapter(100), 6000);
        assertEquals(3, calls.get());
        assertEquals(2, retries());
        assertEquals(2, outcomes("provider_timeout"));
        assertEquals(1, outcomes("success"));
        assertEquals(1, requests.stream().map(r -> r.get("paymentId")).distinct().count());
        assertEquals(3, requests.stream().map(r -> r.get("transactionId")).distinct().count());
        assertTrue(requests.stream().allMatch(r -> Duration.parse(r.get("timeout")).toMillis() == 1200));
    }

    @Test
    void stopsAfterThreeAttempts() {
        replies = n -> new Reply(408, false, "Timeout", 0);
        pay(adapter(100), 6000);
        assertEquals(3, calls.get());
        assertEquals(2, retries());
        assertEquals(3, outcomes("provider_timeout"));
    }

    @ParameterizedTest
    @CsvSource({"200,Temporary error", "200,Timeout", "408,Other rejection", "503,Timeout"})
    void doesNotRetryOtherNegativeResponses(int status, String message) {
        replies = n -> new Reply(status, false, message, 0);
        pay(adapter(100), 6000);
        assertEquals(1, calls.get());
        assertEquals(0, retries());
        assertEquals(1, outcomes("failure"));
    }

    @Test
    void clientTimeoutRetriesAndLateHttpResponseDoesNotProduceAnotherSuccess() throws InterruptedException {
        replies = n -> new Reply(200, true, "", n == 1 ? 1900 : 0);
        var adapter = adapter(100);
        long start = System.nanoTime();
        pay(adapter, 6000);
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(elapsedMillis >= 1400 && elapsedMillis < 3000, "Client timeout should be about 1500 ms");
        assertEquals(2, calls.get());
        assertEquals(1, retries());
        assertEquals(1, outcomes("timeout"));
        assertEquals(1, outcomes("success"));
        assertTrue(lateResponseFinished.await(3, TimeUnit.SECONDS));
        assertEquals(1, outcomes("success"));
        assertEquals(0, registry.get("payments.provider.inflight").gauge().value());
    }

    @Test
    void limiterWaitingCannotConsumeTheNextFullAttemptBudget() {
        replies = n -> new Reply(408, false, "Timeout", 0);
        pay(adapter(1), 2400);
        assertEquals(1, calls.get());
        assertEquals(0, retries(), "A retry that never reached HTTP must not be counted");
    }

    @Test
    void doesNotStartWhenFullAttemptAndReserveDoNotFit() {
        pay(adapter(100), 1600);
        assertEquals(0, calls.get());
        assertEquals(0, retries());
    }

    @Test
    void interruptionStopsWithoutSending() {
        var adapter = adapter(100);
        Thread.currentThread().interrupt();
        try {
            pay(adapter, 6000);
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(0, calls.get());
            assertEquals(0, retries());
        } finally {
            Thread.interrupted();
        }
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
        executor.shutdownNow();
        registry.close();
    }
}
