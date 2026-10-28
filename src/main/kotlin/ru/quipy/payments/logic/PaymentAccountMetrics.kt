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
    private val started = registry.counter("payments.provider.started", tags)
    private val succeeded = registry.counter("payments.provider.requests", tags.and("outcome", "success"))
    private val failed = registry.counter("payments.provider.requests", tags.and("outcome", "failure"))
    private val errors = registry.counter("payments.provider.requests", tags.and("outcome", "error"))

    init {
        Gauge.builder("payments.provider.inflight", inFlight) { it.get().toDouble() }
            .description("Local HTTP slots in use, including connection acquisition and response body consumption")
            .tags(tags).register(registry)
        Gauge.builder("payments.provider.concurrency.limit", properties) { it.parallelRequests.toDouble() }
            .description("Provider account concurrency limit")
            .tags(tags).strongReference(true).register(registry)
        Gauge.builder("payments.provider.rate.limit", properties) { it.rateLimitPerSec.toDouble() }
            .description("Provider account limit on new HTTP requests per second")
            .tags(tags).strongReference(true).register(registry)
    }

    fun requestStarted() {
        started.increment()
    }

    fun requestReserved() {
        if (inFlight.incrementAndGet() > limit) exceeded.increment()
    }

    fun requestFinished(response: ExternalSysResponse?, wasSent: Boolean = true) {
        try {
            if (!wasSent) return
            when {
                response == null -> errors.increment()
                response.result -> succeeded.increment()
                else -> failed.increment()
            }
        } finally {
            inFlight.decrementAndGet()
        }
    }
}
