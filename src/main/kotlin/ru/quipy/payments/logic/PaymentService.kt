package ru.quipy.payments.logic

import java.time.Duration
import java.util.*
import java.util.concurrent.RejectedExecutionException

class PaymentAdmissionRejectedException(message: String) : RejectedExecutionException(message)

enum class PaymentSubmission { STARTED, NOT_STARTED }

interface PaymentService {
    fun checkCanSubmit(deadline: Long)

    /**
     * Returns after at least one account has started HTTP, without waiting for payment completion.
     * Throws if no HTTP request was started; failures after the first launch must not request a retry.
     */
    fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long)
}

/**
 * Adapter for external payment system. Represents the account in the external system.
 *
 * !!! You can extend the interface with additional methods if needed. !!!

 */
interface PaymentExternalSystemAdapter {
    fun checkCanSubmit(deadline: Long)

    /**
     * Waits for concurrency/RPS permits within the deadline budget, then starts HTTP.
     * STARTED means HTTP was launched, not that the payment succeeded.
     * NOT_STARTED means a local transport failure prevented launch. Deadline rejection throws
     * PaymentAdmissionRejectedException; preparation/persistence may also throw before launch.
     * Completion is recorded through payment events.
     */
    fun startPayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): PaymentSubmission

    fun name(): String

    fun price(): Int

    fun isEnabled(): Boolean
}

/**
 * Describes properties of payment-provider accounts.
 */
data class PaymentAccountProperties(
    val serviceName: String,
    val accountName: String,
    val parallelRequests: Int,
    val rateLimitPerSec: Int,
    val price: Int,
    val averageProcessingTime: Duration = Duration.ofSeconds(11),
    val enabled: Boolean,
)

/**
 * Describes response from external service.
 */
class ExternalSysResponse(
    val transactionId: String,
    val paymentId: String,
    val result: Boolean,
    val message: String? = null,
)
