package com.kaizensundays.okhttp

import com.kaizensundays.fusion.messaging.LoadBalancer
import com.kaizensundays.fusion.messaging.Producer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.net.URI
import java.time.Duration

/**
 * Created: Sunday 10/1/2023, 1:36 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
@SuppressWarnings(
    "kotlin:S6508", // Mono<Void>
)
class OkHttpProducer(private val loadBalancer: LoadBalancer, private val builder: OkHttpClient.Builder) : Producer {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    private fun Retry.RetrySignal.printAttempts() = "attempt=" + (this.totalRetries() + 1)

    private fun parse(s: String?, default: Long) = s?.toLong() ?: default

    fun optionsMap(topic: URI): Map<String, String> {

        return if (topic.query != null) {
            topic.query.split("&")
                .map { opt -> opt.split("=") }
                .associate { opt -> opt[0] to opt[1] }
        } else {
            emptyMap()
        }
    }

    fun topicOptions(topic: URI): TopicOptions {
        val opts = TopicOptions()
        val map = optionsMap(topic)
        opts.maxAttempts = parse(map["maxAttempts"], opts.maxAttempts)
        opts.timeoutSec = parse(map["timeoutSec"], opts.timeoutSec)
        return opts
    }

    private fun nextUri(topic: URI): URI {
        val instance = loadBalancer.get().block(Duration.ofSeconds(100))
        requireNotNull(instance)
        val uri = URI("${instance.protocol}://${instance.host}:${instance.port}" + topic.path)
        logger.info("nextUri=$uri")
        return uri
    }

    private fun stream(topic: URI, messages: Flux<ByteArray>, client: OkHttpClient): Flux<ByteArray> {

        return Flux.create { sink ->

            val uri = nextUri(topic)

            val request = Request.Builder()
                .url(uri.toURL())
                .build()

            val listener = object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: okhttp3.Response) {
                    logger.debug("onOpen")
                    messages.map { msg -> webSocket.send(ByteString.of(*msg)) }
                        .subscribe()
                }

                override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
                    logger.debug("onMessage")
                    sink.next(bytes.toByteArray())
                }

                override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                    logger.debug("onClosing")
                    sink.complete()
                }

                override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                    logger.debug("onClosed")
                }

                @Suppress("WRONG_NULLABILITY_FOR_JAVA_OVERRIDE")
                override fun onFailure(webSocket: WebSocket, t: Throwable, response: okhttp3.Response?) {
                    logger.error("onFailure: ${t.message}", t)
                    sink.error(t)
                }
            }
            client.newWebSocket(request, listener)
        }
    }

    override fun request(topic: URI, messages: Flux<ByteArray>): Flux<ByteArray> {

        require("ws" == topic.scheme)

        val opts = topicOptions(topic)

        val client = builder.build()

        return Flux.defer { stream(topic, messages, client) }
            .retryWhen(Retry.backoff(opts.maxAttempts, Duration.ofSeconds(opts.minBackoffSec))
                .maxBackoff(Duration.ofSeconds(opts.maxBackoffSec))
                .doAfterRetry { signal -> logger.trace(signal.printAttempts()) }
            )
    }

    override fun request(topic: URI, msg: ByteArray): Flux<ByteArray> {

        val client = builder.build()

        val request = Request.Builder().url("http://localhost:7701/ping").build()

        val response = client.newCall(request).execute()

        println(response)

        return Flux.empty()
    }

    override fun request(topic: URI): Flux<ByteArray> {
        return request(topic, byteArrayOf())
    }

    private fun send(topic: URI, msg: ByteArray, client: OkHttpClient): Mono<Void> {

        return Mono.create { sink ->

            val uri = nextUri(topic)

            val request = Request.Builder()
                .url(uri.toURL())
                .build()

            val listener = object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: okhttp3.Response) {
                    val byteString = ByteString.of(*msg)
                    webSocket.send(byteString)
                    println("Sent")
                    webSocket.close(1000, "Closing connection")
                    sink.success()
                }

                @Suppress("WRONG_NULLABILITY_FOR_JAVA_OVERRIDE")
                override fun onFailure(webSocket: WebSocket, t: Throwable, response: okhttp3.Response?) {
                    logger.error("onFailure: ${t.message}", t)
                    sink.error(t)
                }
            }
            client.newWebSocket(request, listener)
        }
    }

    override fun send(topic: URI, msg: ByteArray): Mono<Void> {

        require("ws" == topic.scheme)

        val opts = topicOptions(topic)

        val client = builder.build()

        return Mono.defer { send(topic, msg, client) }
            .retryWhen(
                Retry.backoff(opts.maxAttempts, Duration.ofSeconds(opts.minBackoffSec))
                    .maxBackoff(Duration.ofSeconds(opts.maxBackoffSec))
                    .doAfterRetry { signal -> logger.trace(signal.printAttempts()) }
            )
    }
}