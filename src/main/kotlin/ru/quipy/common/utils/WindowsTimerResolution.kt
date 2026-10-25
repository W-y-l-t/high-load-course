package ru.quipy.common.utils

import com.sun.jna.Native
import com.sun.jna.Platform
import com.sun.jna.win32.StdCallLibrary
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/** Keep millisecond pacing from being rounded to Windows' default timer tick. */
@Component
class WindowsTimerResolution(
    @Value("\${payment.windows-high-resolution-timer:true}") enabled: Boolean,
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val timer = if (enabled && Platform.isWindows()) enable() else null

    interface WinMm : StdCallLibrary {
        fun timeBeginPeriod(periodMillis: Int): Int
        fun timeEndPeriod(periodMillis: Int): Int
    }

    private fun enable(): WinMm? {
        try {
            val api = Native.load("winmm", WinMm::class.java)
            val result = api.timeBeginPeriod(1)
            if (result == 0) {
                logger.info("Windows payment timer resolution requested: 1 ms")
                return api
            }
            logger.warn("Windows timer resolution request failed: {}; payment pacing may be slower", result)
        } catch (e: LinkageError) {
            logger.warn("Windows high-resolution timer is unavailable; payment pacing may be slower", e)
        } catch (e: Exception) {
            logger.warn("Cannot request Windows high-resolution timer; payment pacing may be slower", e)
        }
        return null
    }

    @PreDestroy
    fun close() {
        timer?.timeEndPeriod(1)
    }
}
