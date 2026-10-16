package ru.quipy.payments.logic

import ru.quipy.common.utils.RequestMetrics
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.time.Duration
import java.util.concurrent.TimeUnit
import io.micrometer.core.instrument.Tags
import java.util.concurrent.atomic.AtomicInteger

class PaymentAccountMetrics(properties: PaymentAccountProperties, private val registry: MeterRegistry) {
    private val requestMetrics = RequestMetrics(registry)
    private val inFlight = AtomicInteger()
    private val limit = properties.parallelRequests
    private val tags = Tags.of("service", properties.serviceName, "accountName", properties.accountName)
    private val exceeded = registry.counter("payments.provider.concurrency.exceeded", tags)
    private val succeeded = registry.counter("payments.provider.requests", tags.and("outcome", "success"))
    private val failed = registry.counter("payments.provider.requests", tags.and("outcome", "failure"))
    private val errors = registry.counter("payments.provider.requests", tags.and("outcome", "error"))

    private val remainingAtSend = Timer.builder("payments.provider.remaining")
        .description("Remaining client deadline budget immediately before HTTP submission")
        .tags(tags).register(registry)
    private val deadlineRejections = registry.counter("payments.admission.deadline.rejected", tags)
    private val lateResponses = registry.counter("payments.provider.response.late", tags)

    fun registerProcessingReserve(reserve: Duration) {
        Gauge.builder("payments.admission.processing.reserve", reserve) { it.toNanos().toDouble() / 1_000_000_000 }
            .tags(tags).strongReference(true).register(registry)
    }

    fun recordRemaining(millis: Long) = remainingAtSend.record(maxOf(0, millis), TimeUnit.MILLISECONDS)
    fun deadlineRejected() = deadlineRejections.increment()
    fun lateResponse() = lateResponses.increment()

    init {
        Gauge.builder("payments.provider.inflight", inFlight) { it.get().toDouble() }
            .description("Local HTTP calls in progress, including response body consumption")
            .tags(tags).register(registry)
        Gauge.builder("payments.provider.concurrency.limit", properties) { it.parallelRequests.toDouble() }
            .description("Provider account concurrency limit")
            .tags(tags).strongReference(true).register(registry)
    }

    fun requestStarted() {
        requestMetrics.paymentOutgoing.increment()
        if (inFlight.incrementAndGet() > limit) exceeded.increment()
    }

    fun requestFinished(response: ExternalSysResponse?) {
        try {
            when {
                response == null -> errors.increment()
                response.result -> succeeded.increment()
                else -> failed.increment()
            }
        } finally {
            inFlight.decrementAndGet()
            requestMetrics.served.increment()
        }
    }
}
