package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class PaymentQueueMetrics(
    executor: ThreadPoolExecutor,
    capacity: Int,
    serviceName: String,
    registry: MeterRegistry,
) {
    private val queueWait = Timer.builder("payments.queue.wait")
        .description("Time from accepting a payment task to starting its execution")
        .tag("service", serviceName).register(registry)
    private val remainingAtEntry = Timer.builder("payments.admission.remaining")
        .description("Remaining client deadline budget when the request reaches OrderPayer")
        .tag("service", serviceName).register(registry)

    init {
        Gauge.builder("payments.queue.size", executor) { it.queue.size.toDouble() }
            .tag("service", serviceName).register(registry)
        Gauge.builder("payments.queue.capacity", executor) { capacity.toDouble() }
            .tag("service", serviceName).register(registry)
        Gauge.builder("payments.executor.active", executor) { it.activeCount.toDouble() }
            .description("Threads preparing submissions or waiting for semaphore/RPS; excludes asynchronous HTTP")
            .tag("service", serviceName).register(registry)
    }

    fun recordRemaining(millis: Long) = remainingAtEntry.record(maxOf(0, millis), TimeUnit.MILLISECONDS)
    fun recordQueueWait(nanos: Long) = queueWait.record(nanos, TimeUnit.NANOSECONDS)
}
