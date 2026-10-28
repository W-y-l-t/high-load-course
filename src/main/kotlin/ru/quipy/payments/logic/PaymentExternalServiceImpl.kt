package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.RequestMetrics
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.client.BufferingResponseListener
import org.eclipse.jetty.client.Result
import org.eclipse.jetty.http.HttpMethod
import java.time.Duration
import java.util.UUID
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.ceil

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
    private val client: HttpClient,
    queueCapacity: Int,
    resultThreads: Int,
    processingReserveFactor: Double,
    rateWindowMargin: Duration,
) : PaymentExternalSystemAdapter {
    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        private val mapper = ObjectMapper().registerKotlinModule()
    }

    init {
        require(properties.parallelRequests > 0 && properties.rateLimitPerSec > 0)
        require(queueCapacity > 0 && resultThreads > 0)
        require(!properties.averageProcessingTime.isNegative && !properties.averageProcessingTime.isZero)
        require(processingReserveFactor.isFinite() && processingReserveFactor >= 1)
        require(!rateWindowMargin.isNegative)
        mapper.readerFor(ExternalSysResponse::class.java)
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val running = AtomicBoolean(true)
    private val concurrentRequests = Semaphore(properties.parallelRequests, true)
    private val queue = ArrayBlockingQueue<PendingPayment>(queueCapacity)
    private val rateWindow = Duration.ofSeconds(1).plus(rateWindowMargin)
    private val intervalNanos = ceil(rateWindow.toNanos().toDouble() / properties.rateLimitPerSec).toLong()
    private val effectiveIntervalNanos = maxOf(intervalNanos, properties.averageProcessingTime.toNanos() / properties.parallelRequests)
    private val requestRateLimiter = SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), rateWindow)
    private val dispatcher = ScheduledThreadPoolExecutor(1, NamedThreadFactory("payment-dispatch-$accountName")).apply {
        removeOnCancelPolicy = true
        setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
    }
    private val dispatchScheduled = AtomicBoolean()
    private var nextDispatchAt = 0L // Accessed only by the dispatcher thread.

    // One reservation covers a queued/submitted request and its eventual persistence.
    // Thus slow persistence cannot create an unbounded queue of completed HTTP responses.
    private val lifecycleCapacity = (properties.parallelRequests.toLong() + queueCapacity).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
    private val lifecycleSlots = Semaphore(lifecycleCapacity)
    private val resultExecutor = ThreadPoolExecutor(
        minOf(resultThreads, properties.parallelRequests), minOf(resultThreads, properties.parallelRequests),
        0L, TimeUnit.MILLISECONDS, LinkedBlockingQueue(lifecycleCapacity),
        NamedThreadFactory("payment-result-$accountName"), ThreadPoolExecutor.AbortPolicy()
    )
    private val inFlight = ConcurrentHashMap<UUID, CompletableFuture<*>>()
    private val accountMetrics = PaymentAccountMetrics(properties, meterRegistry)
    private val requestMetrics = RequestMetrics(meterRegistry)
    private val initialReserveNanos = (properties.averageProcessingTime.toNanos() * processingReserveFactor).toLong()
    private val timelyDuration = Timer.builder("payments.provider.timely.duration")
        .description("HTTP duration of successful responses received within the client budget")
        .tag("service", serviceName).tag("accountName", accountName)
        .distributionStatisticExpiry(Duration.ofMinutes(1)).distributionStatisticBufferLength(1)
        .register(meterRegistry)
    private val dispatchWait = Timer.builder("payments.dispatch.wait")
        .description("Time from completed preparation to HTTP dispatch")
        .tag("service", serviceName).tag("accountName", accountName).register(meterRegistry)
    private val commitWait = Timer.builder("payments.http.commit.wait")
        .description("HTTP connection acquisition and header write time, excluding provider processing")
        .tag("service", serviceName).tag("accountName", accountName).register(meterRegistry)
    private val remainingAtSend = Timer.builder("payments.provider.remaining")
        .description("Remaining client budget immediately before HTTP submission")
        .tag("service", serviceName).tag("accountName", accountName).register(meterRegistry)
    private val rejected = meterRegistry.counter("payments.dispatch.deadline.rejected",
        "service", serviceName, "accountName", accountName)
    private val late = meterRegistry.counter("payments.provider.response.late",
        "service", serviceName, "accountName", accountName)

    init {
        resultExecutor.prestartAllCoreThreads()
        Gauge.builder("payments.admission.processing.reserve", this) {
            it.processingReserveNanos().toDouble() / 1_000_000_000
        }.tag("service", serviceName).tag("accountName", accountName).register(meterRegistry)
        Gauge.builder("payments.results.pending", resultExecutor) { it.queue.size.toDouble() }
            .tag("service", serviceName).tag("accountName", accountName).register(meterRegistry)
    }

    private class PendingPayment(
        val paymentId: UUID,
        val transactionId: UUID,
        val uri: URI,
        val deadline: PaymentDeadline,
        val queuedAt: Long = System.nanoTime(),
        val admitted: CompletableFuture<Unit> = CompletableFuture(),
        val finished: AtomicBoolean = AtomicBoolean(),
    )

    private fun processingReserveNanos(): Long =
        maxOf(initialReserveNanos, timelyDuration.max(TimeUnit.NANOSECONDS).toLong())

    override fun retryDelayMillis(): Long = maxOf(1, ceil(effectiveIntervalNanos / 1_000_000.0).toLong())

    override fun admissionWaitNanos(deadline: PaymentDeadline, pendingPayments: Int): Long {
        if (!running.get() || !deadline.canFit(processingReserveNanos())) return 0
        val waiting = maxOf(queue.size, pendingPayments)
        // Free sliding-window slots can be used now, including for a burst of arrivals.
        // Charging one pacing interval per queued item rejected useful work too early.
        val rateWait = requestRateLimiter.nanosUntilAvailable(waiting)
        val parallelWait = waiting.toLong() * (properties.averageProcessingTime.toNanos() / properties.parallelRequests)
        return deadline.remainingNanos() - processingReserveNanos() - maxOf(rateWait, parallelWait)
    }

    override fun admissionTimeoutNanos(deadline: PaymentDeadline): Long =
        deadline.remainingNanos() - processingReserveNanos()

    override fun performPaymentAsync(
        paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: PaymentDeadline,
    ): CompletableFuture<Unit> {
        if (admissionWaitNanos(deadline) <= 0) {
            rejected.increment()
            return CompletableFuture.failedFuture(rejection("Insufficient budget for $accountName"))
        }
        if (!lifecycleSlots.tryAcquire()) {
            return CompletableFuture.failedFuture(rejection("Result backlog for $accountName is full"))
        }
        val transactionId = UUID.randomUUID()
        val payment: PendingPayment
        try {
            val uri = URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            // Submission and result events retain the existing provider transaction correlation.
            paymentESService.update(paymentId) {
                it.logSubmission(true, transactionId, now(), Duration.ofMillis(maxOf(0, now() - paymentStartedAt)))
            }
            payment = PendingPayment(paymentId, transactionId, uri, deadline)
        } catch (e: Exception) {
            lifecycleSlots.release()
            return CompletableFuture.failedFuture(e)
        }
        if (!running.get() || !queue.offer(payment)) {
            reject(payment, "Account dispatch queue is full or stopping")
        } else {
            wakeDispatcher()
        }
        return payment.admitted
    }

    private fun wakeDispatcher() {
        if (running.get() && dispatchScheduled.compareAndSet(false, true)) {
            try {
                dispatcher.execute(::dispatch)
            } catch (e: RejectedExecutionException) {
                dispatchScheduled.set(false)
                drainQueue("Payment dispatcher is stopping")
            }
        }
    }

    private fun scheduleNext(delay: Long) {
        if (!running.get()) {
            dispatchScheduled.set(false)
            return
        }
        try {
            dispatcher.schedule(::dispatch, maxOf(0, delay), TimeUnit.NANOSECONDS)
        } catch (e: RejectedExecutionException) {
            dispatchScheduled.set(false)
            drainQueue("Payment dispatcher is stopping")
        }
    }

    private fun dispatch() {
        if (!running.get()) {
            dispatchScheduled.set(false)
            return
        }
        // Expired requests do not occupy paced sending slots.
        while (true) {
            val head = queue.peek() ?: run {
                dispatchScheduled.set(false)
                if (queue.isNotEmpty()) wakeDispatcher()
                return
            }
            if (head.deadline.canFit(processingReserveNanos())) break
            if (queue.remove(head)) {
                rejected.increment()
                reject(head, "Deadline exhausted before HTTP submission")
            }
        }
        val current = System.nanoTime()
        if (nextDispatchAt != 0L && current < nextDispatchAt) {
            scheduleNext(nextDispatchAt - current)
            return
        }
        if (!concurrentRequests.tryAcquire()) {
            scheduleNext(minOf(intervalNanos, TimeUnit.MILLISECONDS.toNanos(10)))
            return
        }
        val permit = requestRateLimiter.reserveUnconfirmed()
        if (permit == null) {
            concurrentRequests.release()
            scheduleNext(requestRateLimiter.nanosUntilAvailable())
            return
        }
        val payment = queue.poll()
        if (payment == null) {
            requestRateLimiter.cancel(permit)
            concurrentRequests.release()
            continueDispatch(null)
        } else {
            // Only one request may be awaiting header commit. Completed payments
            // continue concurrently; they do not block this sending sequence.
            send(payment, permit).whenComplete { committedAt, _ ->
                try {
                    dispatcher.execute { continueDispatch(committedAt) }
                } catch (e: RejectedExecutionException) {
                    dispatchScheduled.set(false)
                    drainQueue("Payment dispatcher is stopping")
                }
            }
        }
    }

    private fun continueDispatch(committedAt: Long?) {
        if (!running.get()) {
            dispatchScheduled.set(false)
            return
        }
        if (committedAt != null) {
            val reserve = processingReserveNanos()
            val waiting = queue.size
            val waitBudget = queue.minOfOrNull { it.deadline.remainingNanos() - reserve }
            // A one-second input burst may have less than a second to leave the
            // queue. Pace it within that budget, keeping an extra sending slot
            // as headroom. The sliding window remains the hard RPS limit.
            val cadence = if (waitBudget == null) intervalNanos else {
                minOf(intervalNanos, maxOf(1L, waitBudget / (waiting + 1)))
            }
            val planned = nextDispatchAt + cadence
            nextDispatchAt = if (nextDispatchAt == 0L || planned <= committedAt) committedAt + cadence else planned
        }
        if (queue.isEmpty()) {
            dispatchScheduled.set(false)
            if (queue.isNotEmpty()) wakeDispatcher()
        } else scheduleNext(nextDispatchAt - System.nanoTime())
    }

    private fun send(payment: PendingPayment, permit: SlidingWindowRateLimiter.Permit): CompletableFuture<Long> {
        val submittingAt = System.nanoTime()
        val committed = CompletableFuture<Long>()
        val handled = CompletableFuture<Unit>()
        val finished = AtomicBoolean()
        val startedAt = AtomicLong()
        inFlight[payment.transactionId] = handled
        handled.whenComplete { _, _ -> inFlight.remove(payment.transactionId) }
        accountMetrics.requestReserved()

        fun finish(body: ExternalSysResponse?, failure: Throwable?) {
            if (!finished.compareAndSet(false, true)) return
            try {
                val wasSent = startedAt.get() != 0L
                // RPS is independent of response duration. Only an unsent reservation is returned.
                if (!wasSent) requestRateLimiter.cancel(permit)
                accountMetrics.requestFinished(body, wasSent)
                if (wasSent) requestMetrics.served.increment()
            } finally {
                concurrentRequests.release()
                try {
                    persist(payment, body, failure)
                } finally {
                    handled.complete(Unit)
                }
            }
        }

        try {
            client.newRequest(payment.uri).method(HttpMethod.POST)
                .timeout(maxOf(1, payment.deadline.remainingNanos()), TimeUnit.NANOSECONDS)
                .onRequestBegin { request ->
                    if (!payment.deadline.tryStartSubmission(processingReserveNanos())) {
                        rejected.increment()
                        request.abort(rejection("Deadline exhausted before writing HTTP headers"))
                    }
                }
                .onRequestCommit {
                    val now = System.nanoTime()
                    startedAt.set(now)
                    requestRateLimiter.confirm(permit)
                    logger.debug("Payment HTTP headers committed: paymentId={}, nanoTime={}", payment.paymentId, now)
                    commitWait.record(now - submittingAt, TimeUnit.NANOSECONDS)
                    dispatchWait.record(now - payment.queuedAt, TimeUnit.NANOSECONDS)
                    remainingAtSend.record(payment.deadline.remainingNanos(), TimeUnit.NANOSECONDS)
                    accountMetrics.requestStarted()
                    requestMetrics.paymentOutgoing.increment()
                    committed.complete(now)
                    payment.admitted.complete(Unit)
                }
                .send(object : BufferingResponseListener() {
                    override fun onComplete(result: Result) {
                        var body: ExternalSysResponse? = null
                        var failure = result.failure
                        try {
                            if (failure == null) {
                                check(result.response.status in 200..299) { "Payment provider HTTP ${result.response.status}" }
                                body = mapper.readValue(contentAsString, ExternalSysResponse::class.java)
                                if (body.result && payment.deadline.remainingNanos() > 0) {
                                    timelyDuration.record(System.nanoTime() - startedAt.get(), TimeUnit.NANOSECONDS)
                                } else if (body.result) late.increment()
                            }
                        } catch (e: Exception) {
                            failure = e
                        } finally {
                            if (!committed.isDone) {
                                val error = failure ?: IllegalStateException("HTTP completed without a header commit")
                                committed.completeExceptionally(error)
                                payment.admitted.completeExceptionally(error)
                            }
                            finish(body, failure)
                        }
                    }
                })
        } catch (e: Exception) {
            committed.completeExceptionally(e)
            payment.admitted.completeExceptionally(e)
            finish(null, e)
        }
        return committed
    }

    private fun reject(payment: PendingPayment, reason: String) {
        val error = rejection(reason)
        payment.admitted.completeExceptionally(error)
        persist(payment, null, error)
    }

    private fun rejection(reason: String) = PaymentAdmissionRejectedException(reason, retryDelayMillis())

    private fun persist(payment: PendingPayment, body: ExternalSysResponse?, error: Throwable?) {
        if (!payment.finished.compareAndSet(false, true)) return
        val succeeded = body?.result == true
        val failure = if (error is CompletionException) error.cause ?: error else error
        val reason = body?.message ?: failure?.message
        val write = Runnable {
            try {
                paymentESService.update(payment.paymentId) {
                    it.logProcessing(succeeded, now(), payment.transactionId, reason)
                }
                when {
                    succeeded -> logger.debug("[{}] Payment succeeded: {}", accountName, payment.paymentId)
                    failure is PaymentAdmissionRejectedException -> logger.debug("[{}] Payment admission rejected: {}, reason={}", accountName, payment.paymentId, reason)
                    else -> logger.warn("[{}] Payment not completed successfully: {}, reason={}", accountName, payment.paymentId, reason)
                }
            } catch (e: Exception) {
                logger.error("[{}] Cannot persist result for payment {}", accountName, payment.paymentId, e)
            } finally {
                lifecycleSlots.release()
            }
        }
        try {
            resultExecutor.execute(write)
        } catch (e: RejectedExecutionException) {
            // Reservations bound the queue in normal operation. Finish writes inline during shutdown.
            write.run()
        }
    }

    private fun drainQueue(reason: String) {
        while (true) reject(queue.poll() ?: return, reason)
    }

    override fun close() {
        if (!running.compareAndSet(true, false)) return
        dispatcher.shutdownNow()
        drainQueue("Payment account is stopping")
        try {
            CompletableFuture.allOf(*inFlight.values.toTypedArray()).get(10, TimeUnit.SECONDS)
        } catch (e: Exception) {
            inFlight.values.forEach { it.cancel(true) }
            if (e is InterruptedException) Thread.currentThread().interrupt()
        }
        resultExecutor.shutdown()
        try {
            if (!resultExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.warn("[{}] Payment result writes are still draining", accountName)
            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
