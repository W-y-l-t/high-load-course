package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class PaymentAccountMetrics(
    properties: PaymentAccountProperties,
    registry: MeterRegistry,
    timeoutPolicy: PaymentTimeoutPolicy,
) {
    private val inFlight = AtomicInteger()
    private val limit = properties.parallelRequests
    private val tags = Tags.of("service", properties.serviceName, "accountName", properties.accountName)
    private val exceeded = registry.counter("payments.provider.concurrency.exceeded", tags)
    private val succeeded = registry.counter("payments.provider.requests", tags.and("outcome", "success"))
    private val failed = registry.counter("payments.provider.requests", tags.and("outcome", "failure"))
    private val errors = registry.counter("payments.provider.requests", tags.and("outcome", "error"))
    private val retries = registry.counter("payments.provider.retries", tags)
    private val durations = listOf("success", "failure", "client_timeout", "provider_timeout", "error").associateWith { outcome ->
        Timer.builder("payments.provider.request.duration")
            .description("HTTP attempt duration through response decoding, excluding queue and limiter waits")
            .tags(tags.and("outcome", outcome))
            .publishPercentiles(0.5, 0.9, 0.95, 0.99)
            .register(registry)
    }

    init {
        Gauge.builder("payments.provider.inflight", inFlight) { it.get().toDouble() }
            .description("Local HTTP calls in progress, including response body consumption")
            .tags(tags).register(registry)
        Gauge.builder("payments.provider.concurrency.limit", properties) { it.parallelRequests.toDouble() }
            .description("Provider account concurrency limit")
            .tags(tags).strongReference(true).register(registry)
        Gauge.builder("payments.provider.timeout.seconds", timeoutPolicy) { it.serverMillis / 1_000.0 }
            .description("Configured per-attempt timeout in seconds")
            .tags(tags.and("source", "provider")).strongReference(true).register(registry)
        Gauge.builder("payments.provider.timeout.seconds", timeoutPolicy) { it.callMillis / 1_000.0 }
            .description("Configured per-attempt timeout in seconds")
            .tags(tags.and("source", "client")).strongReference(true).register(registry)
    }

    internal fun observeRequest(isRetry: Boolean, request: () -> PaymentProviderResponse): PaymentProviderResponse {
        if (inFlight.incrementAndGet() > limit) exceeded.increment()
        val startedAt = System.nanoTime()
        var outcome = "error"
        try {
            if (isRetry) retries.increment()
            val response = request()
            outcome = when {
                response.body.result -> "success"
                response.isTimeout -> "provider_timeout"
                else -> "failure"
            }
            if (response.body.result) succeeded.increment() else failed.increment()
            return response
        } catch (e: Exception) {
            if (isPaymentRequestTimeout(e)) outcome = "client_timeout"
            errors.increment()
            throw e
        } finally {
            inFlight.decrementAndGet()
            durations.getValue(outcome).record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS)
        }
    }
}
