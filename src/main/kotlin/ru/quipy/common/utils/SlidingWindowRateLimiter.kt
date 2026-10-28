package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque
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
    private val permits = ArrayDeque<Long>()

    override fun tick(): Boolean = lock.withLock {
        tryAcquire(System.nanoTime())
    }

    fun tickBlocking() {
        lock.lockInterruptibly()
        try {
            while (true) {
                val now = System.nanoTime()
                if (tryAcquire(now)) return

                val waitNanos = windowNanos - (now - permits.peekFirst())
                nextPermit.awaitNanos(waitNanos)
            }
        } finally {
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
