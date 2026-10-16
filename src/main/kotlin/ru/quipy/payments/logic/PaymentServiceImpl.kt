package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        var started = false
        for (account in paymentAccounts) {
            try {
                if (account.startPayment(paymentId, amount, paymentStartedAt, deadline) == PaymentSubmission.STARTED) {
                    started = true
                }
            } catch (e: Exception) {
                // Once HTTP has started, a retry could duplicate the payment.
                if (!started) throw e
                logger.warn("Skipping remaining accounts after an earlier HTTP submission", e)
                return
            }
        }
        check(started) { "No payment HTTP request was started" }
    }

    override fun checkCanSubmit(deadline: Long) {
        paymentAccounts.forEach { it.checkCanSubmit(deadline) }
    }
}
