package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class PaymentAccountMetrics(properties: PaymentAccountProperties, registry: MeterRegistry) {
    private val inFlight = AtomicInteger()
    private val limit = properties.parallelRequests
    private val tags = Tags.of("service", properties.serviceName, "accountName", properties.accountName)
    private val exceeded = registry.counter("payments.provider.concurrency.exceeded", tags)
    private val succeeded = registry.counter("payments.provider.requests", tags.and("outcome", "success"))
    private val failed = registry.counter("payments.provider.requests", tags.and("outcome", "failure"))
    private val errors = registry.counter("payments.provider.requests", tags.and("outcome", "error"))
    private val retries = registry.counter("payments.provider.retries", tags)
    private val requestDurations = listOf("success", "failure", "timeout", "provider_timeout", "error").associateWith { outcome ->
        Timer.builder("payments.provider.request.duration")
            .description("HTTP attempt duration including response decoding; excludes queue and limiter waiting")
            .tags(tags.and("outcome", outcome))
            .serviceLevelObjectives(
                *longArrayOf(100, 250, 500, 1000, 1200, 1500, 2000, 3000, 4000, 5000, 6000, 8000, 10000)
                    .map { Duration.ofMillis(it) }.toTypedArray()
            )
            .register(registry)
    }

    init {
        Gauge.builder("payments.provider.inflight", inFlight) { it.get().toDouble() }
            .description("Local HTTP calls in progress, including response body consumption")
            .tags(tags).register(registry)
        Gauge.builder("payments.provider.concurrency.limit", properties) { it.parallelRequests.toDouble() }
            .description("Provider account concurrency limit")
            .tags(tags).strongReference(true).register(registry)
    }

    fun observeRequest(isRetry: Boolean, request: () -> PaymentProviderResponse): PaymentProviderResponse {
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
            // OkHttp's whole-call timeout is InterruptedIOException("timeout");
            // socket timeouts use SocketTimeoutException. An interruption is not a timeout.
            if (isPaymentRequestTimeout(e)) {
                outcome = "timeout"
            }
            errors.increment()
            throw e
        } finally {
            inFlight.decrementAndGet()
            requestDurations.getValue(outcome).record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS)
        }
    }
}
