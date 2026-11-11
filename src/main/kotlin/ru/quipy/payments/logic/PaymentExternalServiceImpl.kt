package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.RequestMetrics
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        private val retryDelaysMillis = longArrayOf(0, 0, 0)
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val concurrentRequests = Semaphore(properties.parallelRequests.also {
        require(it > 0) { "Parallel requests must be positive" }
    }, true)
    private val accountMetrics = PaymentAccountMetrics(properties, meterRegistry)
    private val requestMetrics = RequestMetrics(meterRegistry)
    private val requestRateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        if (accountName == "acc-8") Duration.ofMillis(1050) else Duration.ofSeconds(1),
        prioritizeDeadline = accountName == "acc-8",
    )
    private val client = OkHttpClient.Builder().retryOnConnectionFailure(false).build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val temporaryFailure = performAttempt(paymentId, amount, paymentStartedAt, deadline)
        if (!temporaryFailure || accountName != "acc-8") return

        for (delayMillis in retryDelaysMillis) {
            if (delayMillis >= deadline - now()) return
            try {
                Thread.sleep(delayMillis)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            }

            val retryStartedAt = now()
            if (retryStartedAt >= deadline || !performAttempt(paymentId, amount, retryStartedAt, deadline)) return
        }
    }

    private fun performAttempt(paymentId: UUID, amount: Int, startedAt: Long, deadline: Long): Boolean {
        val transactionId = UUID.randomUUID()
        val submittedAt = now()

        // Persist the attempt before contacting the provider so its outcome can be recorded against it.
        paymentESService.update(paymentId) {
            it.logSubmission(submittedAt < deadline, transactionId, submittedAt, Duration.ofMillis(submittedAt - startedAt))
        }

        val body = try {
            callProvider(paymentId, transactionId, amount, deadline)
        } catch (e: Exception) {
            val reason = when (e) {
                is InterruptedException -> "Payment submission interrupted."
                is InterruptedIOException -> "Request timeout."
                else -> e.message
            }
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            try {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = reason)
                }
            } finally {
                if (e is InterruptedException) Thread.currentThread().interrupt()
            }
            return false
        }

        val success = body?.result == true
        val reason = if (body == null) "deadline_exceeded" else body.message
        if (success) {
            logger.info("[{}] Payment succeeded: payment={}, txId={}", accountName, paymentId, transactionId)
        } else {
            logger.warn("[{}] Payment failed: payment={}, txId={}, reason={}", accountName, paymentId, transactionId, reason)
        }
        paymentESService.update(paymentId) {
            it.logProcessing(success, now(), transactionId, reason = reason)
        }
        return !success && reason == "Temporary error"
    }

    private fun callProvider(paymentId: UUID, transactionId: UUID, amount: Int, deadline: Long): ExternalSysResponse? {
        val remainingMillis = deadline - now()
        if (remainingMillis <= 0 || !concurrentRequests.tryAcquire(remainingMillis, TimeUnit.MILLISECONDS)) return null

        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()
            val call = client.newCall(request)
            if (!requestRateLimiter.tryTickBefore(deadline)) return null
            val callBudgetMillis = deadline - now()
            if (callBudgetMillis <= 0) return null

            call.timeout().timeout(callBudgetMillis, TimeUnit.MILLISECONDS)
            logger.debug("[{}] Sending payment: payment={}, txId={}", accountName, paymentId, transactionId)
            return accountMetrics.observeRequest {
                requestMetrics.paymentOutgoing.increment()
                call.execute().use { response ->
                    try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[{}] Invalid payment response: payment={}, txId={}, status={}", accountName, paymentId, transactionId, response.code, e)
                        throw e
                    } finally {
                        requestMetrics.served.increment()
                    }
                }
            }
        } finally {
            concurrentRequests.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
