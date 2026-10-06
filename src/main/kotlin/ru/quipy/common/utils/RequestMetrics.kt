package ru.quipy.common.utils

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

@Component
class RequestMetrics(registry: MeterRegistry) {
    val incoming: Counter = Counter.builder("requests")
        .description("Количество входящих запросов")
        .tags("event", "Incoming")
        .register(registry)

    val paymentOutgoing: Counter = Counter.builder("requests")
        .description("Количество исходящих запросов (в платежную систему)")
        .tags("event", "Payment outgoing")
        .register(registry)

    val served: Counter = Counter.builder("requests")
        .description("Количество выполненных запросов")
        .tags("event", "Served")
        .register(registry)
}