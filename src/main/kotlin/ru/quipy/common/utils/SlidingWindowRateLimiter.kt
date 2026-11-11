package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque
import java.util.PriorityQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
    private val prioritizeDeadline: Boolean = false,
) : RateLimiter {
    init {
        require(rate > 0) { "Rate must be positive" }
        require(!window.isNegative && !window.isZero) { "Window must be positive" }
    }

    private val windowNanos = window.toNanos()
    private val lock = ReentrantLock()
    private val nextPermit = lock.newCondition()
    private val permits = ArrayDeque<Long>()
    private val waiters = PriorityQueue(compareBy<Waiter> { it.deadline }.thenBy { it.arrivalOrder })
    private var nextArrivalOrder = 0L

    private class Waiter(val deadline: Long, val arrivalOrder: Long)

    override fun tick(): Boolean = lock.withLock {
        waiters.isEmpty() && tryAcquire(System.nanoTime())
    }

    fun tryTickBefore(deadline: Long): Boolean {
        lock.lockInterruptibly()
        val waiter = if (prioritizeDeadline) {
            Waiter(deadline, nextArrivalOrder++).also { waiters.add(it) }
        } else {
            null
        }
        try {
            while (true) {
                val nowMillis = System.currentTimeMillis()
                if (deadline <= nowMillis) return false
                val remainingNanos = TimeUnit.MILLISECONDS.toNanos(deadline - nowMillis)

                if (waiter != null && waiters.peek() !== waiter) {
                    nextPermit.awaitNanos(remainingNanos)
                    continue
                }

                val now = System.nanoTime()
                if (tryAcquire(now)) return true

                val waitNanos = windowNanos - (now - permits.peekFirst())
                nextPermit.awaitNanos(minOf(waitNanos, remainingNanos))
            }
        } finally {
            if (waiter != null) {
                val wasFirst = waiters.peek() === waiter
                waiters.remove(waiter)
                if (wasFirst) nextPermit.signalAll()
            }
            lock.unlock()
        }
    }

    private fun tryAcquire(now: Long): Boolean {
        removeExpiredPermits(now)
        if (permits.size.toLong() >= rate) return false

        permits.addLast(now)
        return true
    }

    private fun removeExpiredPermits(now: Long) {
        while (permits.isNotEmpty() && now - permits.peekFirst() >= windowNanos) {
            permits.removeFirst()
        }
    }
}
