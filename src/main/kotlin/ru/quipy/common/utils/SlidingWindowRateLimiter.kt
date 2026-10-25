package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    init {
        require(rate > 0) { "Rate must be positive" }
        require(!window.isNegative && !window.isZero) { "Window must be positive" }
    }

    private val windowNanos = window.toNanos()
    private val lock = ReentrantLock()
    private val nextPermit = lock.newCondition()
    class Permit internal constructor(internal var confirmedAtNanos: Long?)
    private val permits = ArrayDeque<Permit>()

    override fun tick(): Boolean = lock.withLock {
        tryAcquire(System.nanoTime())
    }

    fun nanosUntilAvailable(): Long = lock.withLock {
        val now = System.nanoTime()
        removeExpiredPermits(now)
        waitNanos(now)
    }

    /** Estimate the slot for a new request after the queued requests, without reserving it. */
    fun nanosUntilAvailable(queuedRequests: Int): Long = lock.withLock {
        require(queuedRequests >= 0)
        val now = System.nanoTime()
        removeExpiredPermits(now)
        val position = queuedRequests.toLong()
        val fullWindows = position / rate
        val slot = position % rate
        val freeSlots = rate - permits.size
        val slotWait = if (slot < freeSlots) {
            0L
        } else {
            val permit = permits.elementAt((slot - freeSlots).toInt())
            // A reservation still awaiting header commit cannot expire yet.
            val confirmedAt = permit.confirmedAtNanos ?: now
            maxOf(0L, windowNanos - (now - confirmedAt))
        }
        fullWindows * windowNanos + slotWait
    }

    /** Reserve capacity before sending; start the window when HTTP headers are committed. */
    fun reserveUnconfirmed(): Permit? = lock.withLock {
        removeExpiredPermits(System.nanoTime())
        if (permits.size.toLong() >= rate) null else Permit(null).also { permits.addLast(it) }
    }

    fun confirm(permit: Permit) = lock.withLock {
        if (permit.confirmedAtNanos == null) permit.confirmedAtNanos = System.nanoTime()
        nextPermit.signalAll()
    }

    fun cancel(permit: Permit) = lock.withLock {
        permits.remove(permit)
        nextPermit.signalAll()
    }

    fun tickBlocking() {
        lock.lockInterruptibly()
        try {
            while (true) {
                val now = System.nanoTime()
                if (tryAcquire(now)) return

                nextPermit.awaitNanos(waitNanos(now))
            }
        } finally {
            lock.unlock()
        }
    }

    private fun tryAcquire(now: Long): Boolean {
        removeExpiredPermits(now)
        if (permits.size.toLong() >= rate) return false

        permits.addLast(Permit(now))
        return true
    }

    private fun removeExpiredPermits(now: Long) {
        while (permits.isNotEmpty()) {
            val confirmedAt = permits.peekFirst().confirmedAtNanos ?: return
            if (now - confirmedAt < windowNanos) return
            permits.removeFirst()
        }
    }

    private fun waitNanos(now: Long): Long {
        if (permits.size.toLong() < rate) return 0
        val confirmedAt = permits.peekFirst().confirmedAtNanos ?: return TimeUnit.MILLISECONDS.toNanos(1)
        return maxOf(1, windowNanos - (now - confirmedAt))
    }
}
