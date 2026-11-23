package ru.quipy.payments.logic

import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration

data class PaymentTimeoutPolicy(val serverMillis: Long, val callMillis: Long) {
    init {
        require(serverMillis > 0 && callMillis > serverMillis) { "Call timeout must exceed the positive server timeout" }
    }

    val providerTimeout: String = Duration.ofMillis(serverMillis).toString()
    val maxAttempts = 4

    // Leave time to persist the result after completing the HTTP call.
    fun latestStart(deadline: Long): Long = deadline - callMillis - 200
}

internal data class PaymentProviderResponse(val body: ExternalSysResponse, val status: Int) {
    val isTimeout: Boolean
        get() = status == 408 && !body.result && body.message == "Timeout"
}

internal fun isPaymentRequestTimeout(error: Exception): Boolean =
    error is SocketTimeoutException || (error is InterruptedIOException && error.message == "timeout")
