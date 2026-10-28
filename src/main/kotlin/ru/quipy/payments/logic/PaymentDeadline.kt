package ru.quipy.payments.logic

import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class PaymentAdmissionRejectedException(
    message: String,
    val retryAfterMillis: Long = 1_000L,
) : RejectedExecutionException(message)

/** Keeps the original deadline budget on a monotonic clock after receipt. */
class PaymentDeadline(val epochMillis: Long, receivedAtMillis: Long) {
    private val receivedAtNanos = System.nanoTime()
    private val budgetNanos = TimeUnit.MILLISECONDS.toNanos(maxOf(0, epochMillis - receivedAtMillis))
    private val state = AtomicInteger(WAITING)

    fun remainingNanos(): Long = maxOf(0, budgetNanos - (System.nanoTime() - receivedAtNanos))

    fun canFit(processingNanos: Long): Boolean =
        state.get() != REJECTED && remainingNanos() > processingNanos

    // Expiry and the first irreversible HTTP submission must not both win.
    fun tryStartSubmission(processingNanos: Long): Boolean {
        if (!canFit(processingNanos)) return false
        while (true) {
            when (state.get()) {
                REJECTED -> return false
                SUBMITTED -> return true
                WAITING -> if (state.compareAndSet(WAITING, SUBMITTED)) return true
            }
        }
    }

    fun rejectBeforeSubmission(): Boolean = state.compareAndSet(WAITING, REJECTED)

    companion object {
        private const val WAITING = 0
        private const val SUBMITTED = 1
        private const val REJECTED = 2
    }
}
