package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PreDestroy
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentService: PaymentService,
    meterRegistry: MeterRegistry,
    @Value("\${payment.service-name}") serviceName: String,
    @Value("\${payment.executor.threads:16}") threads: Int,
    @Value("\${payment.queue.capacity:32}") queueCapacity: Int,
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    init {
        require(threads > 0) { "Payment executor threads must be positive" }
        require(queueCapacity > 0) { "Payment queue capacity must be positive" }
    }

    private val paymentExecutor = ThreadPoolExecutor(
        threads,
        threads,
        0L,
        TimeUnit.MILLISECONDS,
        ArrayBlockingQueue(queueCapacity),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy()
    )

    private val metrics = PaymentQueueMetrics(paymentExecutor, queueCapacity, serviceName, meterRegistry)

    init {
        paymentExecutor.prestartAllCoreThreads()
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): CompletableFuture<Long> {
        val createdAt = System.currentTimeMillis()
        metrics.recordRemaining(deadline - createdAt)
        paymentService.checkCanSubmit(deadline)
        val admitted = CompletableFuture<Long>()
        val queuedAt = System.nanoTime()
        paymentExecutor.execute {
            metrics.recordQueueWait(System.nanoTime() - queuedAt)
            try {
                paymentService.checkCanSubmit(deadline)
                val createdEvent = paymentESService.create {
                    it.create(paymentId, orderId, amount)
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                admitted.complete(createdAt)
            } catch (e: Exception) {
                admitted.completeExceptionally(e)
            }
        }
        return admitted
    }

    @PreDestroy
    fun shutdown() {
        paymentExecutor.shutdown()
    }
}
