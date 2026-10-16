package ru.quipy.payments.logic

import java.net.URI
import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8

fun paymentProviderUri(hostPort: String, path: String, vararg parameters: Pair<String, Any>): URI {
    val query = parameters.joinToString("&") { (name, value) ->
        "${URLEncoder.encode(name, UTF_8)}=${URLEncoder.encode(value.toString(), UTF_8)}"
    }
    return URI("http://$hostPort$path?$query")
}
