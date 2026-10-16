package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletionException
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
    private val client: HttpClient,
    processingReserveFactor: Double = 2.1,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val concurrentRequests = Semaphore(properties.parallelRequests.also {
        require(it > 0) { "Parallel requests must be positive" }
    }, true)
    private val accountMetrics = PaymentAccountMetrics(properties, meterRegistry)
    private val requestRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val processingReserveNanos = (properties.averageProcessingTime.toNanos() * processingReserveFactor).toLong().also {
        require(processingReserveFactor.isFinite() && processingReserveFactor >= 1) { "Processing reserve factor must be finite and at least one" }
        require(it > 0) { "Average processing time must be positive" }
    }

    init {
        accountMetrics.registerProcessingReserve(Duration.ofNanos(processingReserveNanos))
    }

    private fun waitBudgetNanos(deadline: Long): Long =
        TimeUnit.MILLISECONDS.toNanos(maxOf(0, deadline - now())) - processingReserveNanos

    private fun rejectDeadline(): Nothing {
        accountMetrics.deadlineRejected()
        throw PaymentAdmissionRejectedException("Insufficient deadline budget for account $accountName")
    }

    override fun checkCanSubmit(deadline: Long) {
        if (waitBudgetNanos(deadline) <= 0) rejectDeadline()
    }

    override fun startPayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): PaymentSubmission {
        checkCanSubmit(deadline)
        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        try {
            val request = HttpRequest.newBuilder()
                .uri(paymentProviderUri(paymentProviderHostPort, "/external/process",
                    "serviceName" to serviceName, "token" to token, "accountName" to accountName,
                    "transactionId" to transactionId, "paymentId" to paymentId, "amount" to amount))
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build()

            val waitBudget = waitBudgetNanos(deadline)
            if (waitBudget <= 0 || !concurrentRequests.tryAcquire(waitBudget, TimeUnit.NANOSECONDS)) rejectDeadline()
            try {
                val rateBudget = waitBudgetNanos(deadline)
                if (rateBudget <= 0 || !requestRateLimiter.tryTickBlocking(Duration.ofNanos(rateBudget))) rejectDeadline()
                checkCanSubmit(deadline)
                logger.debug("[{}] Sending payment: payment={}, txId={}", accountName, paymentId, transactionId)
                accountMetrics.recordRemaining(deadline - now())
                accountMetrics.requestStarted()
                val response = try {
                    client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                } catch (e: Exception) {
                    accountMetrics.requestFinished(null)
                    throw e
                }

                // The permit belongs to the HTTP operation until its full body is received.
                // Returning from this method frees an OrderPayer thread, not this permit.
                response.whenComplete { httpResponse, error ->
                    completeRequest(paymentId, transactionId, httpResponse, error, deadline)
                }
                return PaymentSubmission.STARTED
            } catch (e: Exception) {
                // No callback was installed, so this thread still owns the permit.
                concurrentRequests.release()
                throw e
            }
        } catch (e: Exception) {
            try {
                recordResult(paymentId, transactionId, null, e)
                if (e is PaymentAdmissionRejectedException) throw e
                return PaymentSubmission.NOT_STARTED
            } finally {
                if (e is InterruptedException) Thread.currentThread().interrupt()
            }
        }
    }

    private fun completeRequest(
        paymentId: UUID,
        transactionId: UUID,
        response: HttpResponse<String>?,
        error: Throwable?,
        deadline: Long,
    ) {
        var body: ExternalSysResponse? = null
        var failure = error
        try {
            if (error == null) {
                checkNotNull(response) { "Empty payment response" }
                check(response.statusCode() in 200..299) { "Payment provider HTTP ${response.statusCode()}" }
                body = mapper.readValue(response.body(), ExternalSysResponse::class.java)
                if (body.result && now() > deadline) accountMetrics.lateResponse()
            }
        } catch (e: Exception) {
            failure = e
        } finally {
            try {
                accountMetrics.requestFinished(body)
            } finally {
                concurrentRequests.release()
            }
        }
        // The provider permit has already been released before persistence.
        recordResult(paymentId, transactionId, body, failure)
    }

    private fun recordResult(paymentId: UUID, transactionId: UUID, body: ExternalSysResponse?, error: Throwable?) {
        val failure = if (error is CompletionException) error.cause ?: error else error
        val succeeded = body?.result == true
        val reason = body?.message ?: failure?.message
        if (succeeded) {
            logger.info("[{}] Payment succeeded: payment={}, txId={}", accountName, paymentId, transactionId)
        } else {
            logger.warn("[{}] Payment failed: payment={}, txId={}, reason={}", accountName, paymentId, transactionId, reason)
        }
        try {
            paymentESService.update(paymentId) {
                it.logProcessing(succeeded, now(), transactionId, reason = reason)
            }
        } catch (e: Exception) {
            logger.error("[{}] Cannot persist payment result: payment={}, txId={}", accountName, paymentId, transactionId, e)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
