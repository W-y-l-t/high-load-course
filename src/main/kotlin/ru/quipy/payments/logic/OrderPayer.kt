package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import jakarta.annotation.PreDestroy
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.UUID
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Service
class OrderPayer(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentService: PaymentService,
    meterRegistry: MeterRegistry,
    @Value("\${payment.service-name}") serviceName: String,
    @Value("\${payment.executor.threads:8}") threads: Int,
    @Value("\${payment.queue.capacity:128}") queueCapacity: Int,
) {
    init {
        require(threads > 0 && queueCapacity > 0) { "Payment threads and queue capacity must be positive" }
    }

    // A slot covers preparation and dispatch waiting, not the asynchronous HTTP response.
    private val admissionSlots = Semaphore(queueCapacity)
    private val admissionLock = ReentrantLock()
    private val pending = AtomicInteger()
    private val admissionFutures = ConcurrentHashMap<UUID, CompletableFuture<Long>>()
    private val preparationExecutor = ThreadPoolExecutor(
        threads, threads, 0L, TimeUnit.MILLISECONDS, ArrayBlockingQueue(queueCapacity),
        NamedThreadFactory("payment-preparation"), ThreadPoolExecutor.AbortPolicy()
    )
    private val expiryExecutor = ScheduledThreadPoolExecutor(1, NamedThreadFactory("payment-admission-expiry")).apply {
        removeOnCancelPolicy = true
        setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
    }
    private val queueWait = Timer.builder("payments.queue.wait")
        .description("Time waiting for a payment preparation thread")
        .tag("service", serviceName).register(meterRegistry)
    private val remainingAtEntry = Timer.builder("payments.admission.remaining")
        .description("Client deadline budget at entry")
        .tag("service", serviceName).register(meterRegistry)
    private val expired = meterRegistry.counter("payments.admission.deadline.rejected", "service", serviceName)

    init {
        preparationExecutor.prestartAllCoreThreads()
        Gauge.builder("payments.queue.size", pending) { it.get().toDouble() }
            .description("Payments awaiting HTTP admission, including preparation and dispatcher waiting")
            .tag("service", serviceName).register(meterRegistry)
        Gauge.builder("payments.queue.capacity", admissionSlots) { queueCapacity.toDouble() }
            .description("Maximum number of payments awaiting HTTP admission")
            .tag("service", serviceName).register(meterRegistry)
        Gauge.builder("payments.executor.active", preparationExecutor) { it.activeCount.toDouble() }
            .description("Threads doing payment preparation and event-store I/O")
            .tag("service", serviceName).register(meterRegistry)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): CompletableFuture<Long> {
        val createdAt = System.currentTimeMillis()
        val budget = PaymentDeadline(deadline, createdAt)
        remainingAtEntry.record(budget.remainingNanos(), TimeUnit.NANOSECONDS)
        admissionLock.withLock {
            // Include work still awaiting preparation, before doing any event-store I/O.
            // Serialize only this short reservation so simultaneous arrivals cannot all
            // observe the same free position in the deadline-limited buffer.
            if (paymentService.admissionWaitNanos(budget, pending.get()) <= 0) {
                expired.increment()
                throw rejection("Insufficient deadline budget")
            }
            if (!admissionSlots.tryAcquire()) throw rejection("Payment buffer is full")
            pending.incrementAndGet()
        }

        val admitted = CompletableFuture<Long>()
        val queuedAt = System.nanoTime()
        admissionFutures[paymentId] = admitted
        val prepare = FutureTask<Unit> {
            queueWait.record(System.nanoTime() - queuedAt, TimeUnit.NANOSECONDS)
            try {
                if (!admitted.isDone) {
                    if (paymentService.admissionWaitNanos(budget) <= 0) {
                        throw rejection("Deadline exhausted before preparation")
                    }
                    paymentESService.create { it.create(paymentId, orderId, amount) }
                    if (!admitted.isDone) {
                        paymentService.submitPaymentRequest(paymentId, amount, createdAt, budget)
                            .whenComplete { _, error ->
                                if (error == null) admitted.complete(createdAt)
                                else admitted.completeExceptionally(error)
                            }
                    }
                }
            } catch (e: Exception) {
                admitted.completeExceptionally(e)
            }
            Unit
        }
        val expiry = AtomicReference<ScheduledFuture<*>?>()
        // Publish admission to the controller only after releasing the waiting slot.
        // MVC response callbacks must not hold capacity for an already sent payment.
        val response = admitted.whenComplete { _, error ->
            expiry.get()?.cancel(false)
            if (error != null) {
                prepare.cancel(false)
                preparationExecutor.remove(prepare)
            }
            admissionFutures.remove(paymentId)
            pending.decrementAndGet()
            admissionSlots.release()
        }
        try {
            expiry.set(expiryExecutor.schedule({
                if (!admitted.isDone && budget.rejectBeforeSubmission()) {
                    expired.increment()
                    admitted.completeExceptionally(rejection("Deadline exhausted while awaiting admission"))
                }
            }, maxOf(0, paymentService.admissionTimeoutNanos(budget)), TimeUnit.NANOSECONDS))
            if (admitted.isDone) expiry.get()?.cancel(false) else preparationExecutor.execute(prepare)
        } catch (e: RejectedExecutionException) {
            budget.rejectBeforeSubmission()
            admitted.completeExceptionally(rejection("Payment preparation is stopping or full"))
        }
        return response
    }

    private fun rejection(reason: String) = PaymentAdmissionRejectedException(reason, paymentService.retryDelayMillis())

    @PreDestroy
    fun shutdown() {
        preparationExecutor.shutdown()
        expiryExecutor.shutdown()
        try {
            if (!preparationExecutor.awaitTermination(10, TimeUnit.SECONDS)) preparationExecutor.shutdownNow()
        } catch (e: InterruptedException) {
            preparationExecutor.shutdownNow()
            Thread.currentThread().interrupt()
        }
        admissionFutures.values.forEach {
            it.completeExceptionally(IllegalStateException("Payment service is stopping"))
        }
    }
}
