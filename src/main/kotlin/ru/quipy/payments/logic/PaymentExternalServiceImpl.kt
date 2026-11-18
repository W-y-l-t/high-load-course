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
        private const val MAX_ATTEMPTS = 3
        private const val CLIENT_TIMEOUT_MILLIS = 1500L
        private const val RESULT_RESERVE_MILLIS = 200L
        private val SERVER_TIMEOUT = Duration.ofMillis(1200).toString()
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
        Duration.ofMillis(1050),
        prioritizeDeadline = true,
    )
    private val client = OkHttpClient.Builder().retryOnConnectionFailure(false).build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (attempt in 0 until MAX_ATTEMPTS) {
            if (Thread.currentThread().isInterrupted || deadline - now() < CLIENT_TIMEOUT_MILLIS + RESULT_RESERVE_MILLIS) return
            val startedAt = if (attempt == 0) paymentStartedAt else now()
            if (!performAttempt(paymentId, amount, startedAt, deadline, isRetry = attempt > 0)) return
        }
    }

    private fun performAttempt(paymentId: UUID, amount: Int, startedAt: Long, deadline: Long, isRetry: Boolean): Boolean {
        val transactionId = UUID.randomUUID()
        val submittedAt = now()

        // Persist the attempt before contacting the provider so its outcome can be recorded against it.
        paymentESService.update(paymentId) {
            it.logSubmission(submittedAt < deadline, transactionId, submittedAt, Duration.ofMillis(submittedAt - startedAt))
        }

        val response = try {
            callProvider(paymentId, transactionId, amount, deadline, isRetry)
        } catch (e: Exception) {
            val reason = when (e) {
                is InterruptedException -> "Payment submission interrupted."
                else -> if (isPaymentRequestTimeout(e)) "Request timeout." else e.message
            }
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            try {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = reason)
                }
            } finally {
                if (e is InterruptedException) Thread.currentThread().interrupt()
            }
            return isPaymentRequestTimeout(e) && !Thread.currentThread().isInterrupted
        }

        val body = response?.body
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
        return response?.isTimeout == true
    }

    private fun callProvider(paymentId: UUID, transactionId: UUID, amount: Int, deadline: Long, isRetry: Boolean): PaymentProviderResponse? {
        // Leave enough time for a full HTTP attempt and recording its outcome.
        val latestStart = deadline - CLIENT_TIMEOUT_MILLIS - RESULT_RESERVE_MILLIS
        val remainingMillis = latestStart - now()
        if (remainingMillis <= 0 || !concurrentRequests.tryAcquire(remainingMillis, TimeUnit.MILLISECONDS)) return null

        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=$SERVER_TIMEOUT")
                post(emptyBody)
            }.build()
            val call = client.newCall(request)
            if (!requestRateLimiter.tryTickBefore(latestStart)) return null
            if (now() >= latestStart || Thread.currentThread().isInterrupted) return null

            call.timeout().timeout(CLIENT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
            logger.debug("[{}] Sending payment: payment={}, txId={}", accountName, paymentId, transactionId)
            return accountMetrics.observeRequest(isRetry) {
                requestMetrics.paymentOutgoing.increment()
                call.execute().use { response ->
                    try {
                        PaymentProviderResponse(
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java),
                            response.code,
                        )
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
