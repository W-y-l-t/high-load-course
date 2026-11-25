package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.Call
import okhttp3.EventListener
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
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
    private val timeoutPolicy: PaymentTimeoutPolicy,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = ByteArray(0).toRequestBody()
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val concurrentRequests = Semaphore(properties.parallelRequests.also {
        require(it > 0) { "Parallel requests must be positive" }
    }, true)
    private val accountMetrics = PaymentAccountMetrics(properties, meterRegistry, timeoutPolicy)
    private val requestMetrics = RequestMetrics(meterRegistry)
    private val requestRateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofMillis(1050),
        prioritizeDeadline = true,
    )
    private val client = OkHttpClient.Builder()
        .retryOnConnectionFailure(false)
        .followRedirects(false)
        .eventListener(object : EventListener() {
            override fun requestHeadersStart(call: Call) {
                call.request().tag(Transmission::class.java)?.headersStarted = true
            }
        })
        .build()

    private class Transmission {
        var headersStarted = false
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val latestStart = timeoutPolicy.latestStart(deadline)
        for (attempt in 0 until timeoutPolicy.maxAttempts) {
            if (Thread.currentThread().isInterrupted || (attempt > 0 && now() >= latestStart)) return
            val startedAt = if (attempt == 0) paymentStartedAt else now()
            if (!performAttempt(paymentId, amount, startedAt, latestStart, attempt > 0)) return
        }
    }

    private fun performAttempt(
        paymentId: UUID, amount: Int, startedAt: Long, latestStart: Long, isRetry: Boolean,
    ): Boolean {
        val transactionId = UUID.randomUUID()
        val submittedAt = now()

        // Persist the attempt before contacting the provider so its outcome can be recorded against it.
        paymentESService.update(paymentId) {
            it.logSubmission(submittedAt < latestStart, transactionId, submittedAt, Duration.ofMillis(submittedAt - startedAt))
        }

        val transmission = Transmission()
        val response = try {
            callProvider(paymentId, transactionId, amount, latestStart, isRetry, transmission)
        } catch (e: Exception) {
            val timedOut = isPaymentRequestTimeout(e)
            val reason = when (e) {
                is InterruptedException -> "Payment submission interrupted."
                else -> when {
                    timedOut && transmission.headersStarted -> "Request timeout; payment result unknown."
                    timedOut -> "Request timeout before sending headers."
                    else -> "Payment HTTP error: ${e.javaClass.simpleName}."
                }
            }
            logger.warn("[{}] Payment call failed: payment={}, txId={}, reason={}", accountName, paymentId, transactionId, reason)
            try {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = reason)
                }
            } finally {
                if (e is InterruptedException) Thread.currentThread().interrupt()
            }
            return timedOut && !transmission.headersStarted && !Thread.currentThread().isInterrupted
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
        if (response?.isTimeout == true) return true
        return !success && reason == "Temporary error"
    }

    private fun callProvider(
        paymentId: UUID, transactionId: UUID, amount: Int, latestStart: Long,
        isRetry: Boolean, transmission: Transmission,
    ): PaymentProviderResponse? {
        val remainingMillis = latestStart - now()
        if (remainingMillis <= 0 || !concurrentRequests.tryAcquire(remainingMillis, TimeUnit.MILLISECONDS)) return null

        try {
            val url = "http://$paymentProviderHostPort/external/process".toHttpUrl().newBuilder()
                .addQueryParameter("serviceName", serviceName)
                .addQueryParameter("token", token)
                .addQueryParameter("accountName", accountName)
                .addQueryParameter("transactionId", transactionId.toString())
                .addQueryParameter("paymentId", paymentId.toString())
                .addQueryParameter("amount", amount.toString())
                .addQueryParameter("timeout", timeoutPolicy.providerTimeout)
            val request = Request.Builder().url(url.build()).post(emptyBody)
                .tag(Transmission::class.java, transmission).build()
            val call = client.newCall(request)
            if (!requestRateLimiter.tryTickBefore(latestStart)) return null
            if (now() >= latestStart || Thread.currentThread().isInterrupted) return null

            call.timeout().timeout(timeoutPolicy.callMillis, TimeUnit.MILLISECONDS)
            logger.debug("[{}] Sending payment: payment={}, txId={}", accountName, paymentId, transactionId)
            return accountMetrics.observeRequest(isRetry) {
                requestMetrics.paymentOutgoing.increment()
                call.execute().use { response ->
                    try {
                        val body = mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        check(body.paymentId == paymentId.toString() && body.transactionId == transactionId.toString()) {
                            "Unmatched payment response"
                        }
                        check(response.isSuccessful || !body.result) { "Inconsistent payment response" }
                        PaymentProviderResponse(body, response.code)
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
