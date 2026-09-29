package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import java.util.concurrent.atomic.AtomicInteger

class PaymentAccountMetrics(properties: PaymentAccountProperties, registry: MeterRegistry) {
    private val inFlight = AtomicInteger()
    private val limit = properties.parallelRequests
    private val tags = Tags.of("service", properties.serviceName, "accountName", properties.accountName)
    private val exceeded = registry.counter("payments.provider.concurrency.exceeded", tags)
    private val succeeded = registry.counter("payments.provider.requests", tags.and("outcome", "success"))
    private val failed = registry.counter("payments.provider.requests", tags.and("outcome", "failure"))
    private val errors = registry.counter("payments.provider.requests", tags.and("outcome", "error"))

    init {
        Gauge.builder("payments.provider.inflight", inFlight) { it.get().toDouble() }
            .description("Local HTTP calls in progress, including response body consumption")
            .tags(tags).register(registry)
        Gauge.builder("payments.provider.concurrency.limit", properties) { it.parallelRequests.toDouble() }
            .description("Provider account concurrency limit")
            .tags(tags).strongReference(true).register(registry)
    }

    fun observeRequest(request: () -> ExternalSysResponse): ExternalSysResponse {
        if (inFlight.incrementAndGet() > limit) exceeded.increment()
        try {
            val response = request()
            if (response.result) succeeded.increment() else failed.increment()
            return response
        } catch (e: Exception) {
            errors.increment()
            throw e
        } finally {
            inFlight.decrementAndGet()
        }
    }
}
