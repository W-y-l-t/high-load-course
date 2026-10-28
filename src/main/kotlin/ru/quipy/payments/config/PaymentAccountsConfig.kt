package ru.quipy.payments.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.eclipse.jetty.client.HttpClient
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.*
import java.net.URI
import java.util.*
import java.time.Duration
import java.util.concurrent.TimeUnit


@Configuration
@DependsOn("windowsTimerResolution")
class PaymentAccountsConfig {
    companion object {
        private val mapper = ObjectMapper().registerKotlinModule().registerModules(JavaTimeModule())
    }

    @Value("\${payment.hostPort}")
    lateinit var paymentProviderHostPort: String

    @Value("\${payment.service-name}")
    lateinit var serviceName: String

    @Value("\${payment.token}")
    lateinit var token: String

    @Value("#{'\${payment.accounts}'.split(',')}")
    lateinit var allowedAccounts: List<String>

    @Value("\${payment.queue.capacity:128}")
    var queueCapacity: Int = 128

    @Value("\${payment.results.threads:4}")
    var resultThreads: Int = 4

    @Value("\${payment.processing-reserve-factor:2.1}")
    var processingReserveFactor: Double = 2.1

    @Value("\${payment.rate-window-margin-millis:10}")
    var rateWindowMarginMillis: Long = 10

    @Bean
    fun paymentAccountProperties(): List<PaymentAccountProperties> {
        // Load the limits before constructing the payment connection pool.
        // This startup-only request does not process or charge a payment.
        val responseBody = HttpClient().use { bootstrap ->
            bootstrap.connectTimeout = 5_000
            bootstrap.start()
            val response = bootstrap.newRequest(URI("http://$paymentProviderHostPort/external/accounts?serviceName=$serviceName&token=$token"))
                .timeout(5, TimeUnit.SECONDS).send()
            check(response.status in 200..299) { "Cannot load payment account settings: HTTP ${response.status}" }
            response.contentAsString
        }
        return mapper.readValue<List<PaymentAccountProperties>>(
            responseBody,
            mapper.typeFactory.constructCollectionType(List::class.java, PaymentAccountProperties::class.java)
        ).filter { it.accountName in allowedAccounts }
            .map { it.copy(enabled = true) }
            .onEach(::println)
    }

    @Bean(destroyMethod = "stop")
    fun paymentHttpClient(paymentAccountProperties: List<PaymentAccountProperties>): HttpClient = HttpClient().apply {
        name = "payment-provider-http"
        connectTimeout = 5_000
        idleTimeout = 0 // Each payment has its own timeout from the original deadline.
        isFollowRedirects = false
        // Jetty's default pool of 64 would cap a one-second provider near 64 RPS.
        // Semaphore limits bound real concurrency; the pool must accommodate them.
        maxConnectionsPerDestination = paymentAccountProperties.sumOf { it.parallelRequests.toLong() }
            .coerceIn(1L, Int.MAX_VALUE.toLong()).toInt()
        maxRequestsQueuedPerDestination = queueCapacity
        start()
    }

    @Bean
    fun accountAdapters(paymentService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>, meterRegistry: MeterRegistry, paymentHttpClient: HttpClient, paymentAccountProperties: List<PaymentAccountProperties>): List<PaymentExternalSystemAdapter> {
        return paymentAccountProperties
            .map {
                PaymentExternalSystemAdapterImpl(
                    it,
                    paymentService,
                    paymentProviderHostPort,
                    token,
                    meterRegistry,
                    paymentHttpClient,
                    queueCapacity,
                    resultThreads,
                    processingReserveFactor,
                    Duration.ofMillis(rateWindowMarginMillis),
                )
            }
    }
}
