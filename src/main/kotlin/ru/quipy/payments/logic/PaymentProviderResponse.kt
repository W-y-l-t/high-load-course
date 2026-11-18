package ru.quipy.payments.logic

import java.io.InterruptedIOException
import java.net.SocketTimeoutException

data class PaymentProviderResponse(val body: ExternalSysResponse, val statusCode: Int) {
    val isTimeout: Boolean
        get() = statusCode == 408 && !body.result && body.message == "Timeout"
}

internal fun isPaymentRequestTimeout(error: Exception): Boolean =
    error is SocketTimeoutException || (error is InterruptedIOException && error.message == "timeout")
