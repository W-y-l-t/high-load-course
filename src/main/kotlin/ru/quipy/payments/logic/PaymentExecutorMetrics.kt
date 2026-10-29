package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.ThreadPoolExecutor

@Component
class PaymentExecutorMetrics(
    private val meterRegistry: MeterRegistry,
    @param:Value("\${payment.service-name}") private val serviceName: String,
) {
    fun bindTo(executor: ThreadPoolExecutor) {
        ExecutorServiceMetrics(
            executor,
            "payment-submission-executor",
            Tags.of("service", serviceName),
        ).bindTo(meterRegistry)
    }
}
