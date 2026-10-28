package ru.quipy.payments.logic

import jakarta.annotation.PreDestroy
import org.springframework.stereotype.Service
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

@Service
class PaymentSystemImpl(private val paymentAccounts: List<PaymentExternalSystemAdapter>) : PaymentService {
    override fun retryDelayMillis(): Long =
        paymentAccounts.filter { it.isEnabled() }.minOfOrNull { it.retryDelayMillis() } ?: 1_000L

    override fun admissionWaitNanos(deadline: PaymentDeadline, pendingPayments: Int): Long =
        paymentAccounts.filter { it.isEnabled() }.maxOfOrNull { it.admissionWaitNanos(deadline, pendingPayments) } ?: 0L

    override fun admissionTimeoutNanos(deadline: PaymentDeadline): Long =
        paymentAccounts.filter { it.isEnabled() }.maxOfOrNull { it.admissionTimeoutNanos(deadline) } ?: 0L

    override fun submitPaymentRequest(
        paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: PaymentDeadline,
    ): CompletableFuture<Unit> {
        val accounts = paymentAccounts.filter { it.isEnabled() }
        if (accounts.isEmpty()) return CompletableFuture.failedFuture(PaymentAdmissionRejectedException("No enabled payment accounts"))
        val admitted = CompletableFuture<Unit>()
        val remaining = AtomicInteger(accounts.size)
        val firstError = AtomicReference<Throwable>()
        // Preserve attempts on configured accounts; never ask for a retry after one was sent.
        accounts.forEach { account ->
            val submission = try {
                account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
            } catch (e: Exception) {
                CompletableFuture.failedFuture(e)
            }
            submission.whenComplete { _, error ->
                if (error == null) admitted.complete(Unit) else firstError.compareAndSet(null, error)
                if (remaining.decrementAndGet() == 0 && !admitted.isDone) {
                    admitted.completeExceptionally(firstError.get())
                }
            }
        }
        return admitted
    }

    @PreDestroy
    fun shutdown() = paymentAccounts.forEach { it.close() }
}
