package com.kaizensundays.okhttp

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.time.Duration

/**
 * Created: Saturday 10/14/2023, 6:54 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
abstract class TestSupport {

    val logger: Logger = LoggerFactory.getLogger(javaClass)

    fun ByteArray?.asText() = if (this != null) String(this) else ""

    fun <T> Flux<T>.blockLast(sec: Long): T? = blockLast(Duration.ofSeconds(sec))

}