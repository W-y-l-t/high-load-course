package ru.quipy.apigateway

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException
import ru.quipy.common.utils.RequestMetrics
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import ru.quipy.payments.logic.PaymentAdmissionRejectedException
import java.util.*
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException

@RestController
class APIController(
        private val meterRegistry: MeterRegistry,
        private val requestMetrics: RequestMetrics) {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    private val rejectedPayments = meterRegistry.counter("payments.admission.rejected")
    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        if (price <= 0) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Price must be positive")
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )
        return orderRepository.save(order)
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): CompletableFuture<ResponseEntity<PaymentSubmissionDto>> {
        requestMetrics.incoming.increment()
        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "No such order $orderId")
        if (deadline <= 0) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Deadline must be an epoch timestamp")

        return try {
            orderPayer.processPayment(orderId, order.price, paymentId, deadline).handle { createdAt, error ->
                if (error == null) {
                    orderRepository.save(order.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
                    ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
                } else {
                    var cause = error
                    while (cause is CompletionException && cause.cause != null) cause = cause.cause!!
                    if (cause is RejectedExecutionException) rejectionResponse(cause)
                    else throw CompletionException(cause)
                }
            }
        } catch (e: RejectedExecutionException) {
            CompletableFuture.completedFuture(rejectionResponse(e))
        }
    }

    private fun rejectionResponse(error: RejectedExecutionException): ResponseEntity<PaymentSubmissionDto> {
        rejectedPayments.increment()
        logger.debug("Payment admission rejected; returning HTTP 429: {}", error.message)
        val retryDelay = (error as? PaymentAdmissionRejectedException)?.retryAfterMillis ?: 1_000L
        // Bombardier's contract expects an epoch timestamp in milliseconds.
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
            .header(HttpHeaders.RETRY_AFTER, (System.currentTimeMillis() + maxOf(1, retryDelay)).toString())
            .build()
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}
