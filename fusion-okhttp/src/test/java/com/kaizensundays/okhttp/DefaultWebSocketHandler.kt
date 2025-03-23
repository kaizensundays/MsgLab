package com.kaizensundays.okhttp

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.core.io.buffer.DataBuffer
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers

/**
 * Created: Saturday 9/30/2023, 7:28 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
class DefaultWebSocketHandler : WebSocketHandler {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    private val topic = Sinks.many().multicast().directBestEffort<ByteArray>()

    private fun handle(msg: ByteArray) {
        logger.info("<<< {}", String(msg))
        topic.tryEmitNext(msg)
    }

    private fun readBytes(data: DataBuffer): ByteArray {
        val bytes = ByteArray(data.readableByteCount())
        data.read(bytes)
        return bytes
    }

    override fun handle(session: WebSocketSession): Mono<Void> {

        val sub = session.receive()
            .map { wsm -> readBytes(wsm.payload) }
            .publishOn(Schedulers.boundedElastic())
            .log()
            .doOnNext { msg -> handle(msg) }
            .subscribeOn(Schedulers.boundedElastic())
            .then()

        val pub = session.send(
            topic.asFlux()
                .publishOn(Schedulers.boundedElastic())
                .doOnNext { msg ->
                    logger.info(">>> {}", String(msg))
                }
                .map { msg -> session.binaryMessage { factory -> factory.wrap(msg) } }
                .subscribeOn(Schedulers.boundedElastic())
        )

        return Mono.zip(sub, pub).then();
    }
}
