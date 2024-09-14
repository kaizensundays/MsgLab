package com.kaizensundays.fusion.okhttp

import com.kaizensundays.fusion.messaging.DefaultLoadBalancer
import com.kaizensundays.fusion.messaging.Instance
import com.kaizensundays.fusion.messaging.LoadBalancer
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.test.context.ContextConfiguration
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.net.URI
import java.time.Duration
import kotlin.test.assertTrue

/**
 * Created: Sunday 10/1/2023, 1:38 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
@ContextConfiguration(locations = ["/OkHttpProducerIntegrationTest.xml"])
class OkHttpProducerIntegrationTest : IntegrationTestSupport() {

    private lateinit var loadBalancer: LoadBalancer

    private lateinit var producer: OkHttpProducer

    @LocalServerPort
    var port = 0

    @BeforeEach
    fun before() {
        loadBalancer = DefaultLoadBalancer(
            listOf(
                Instance("localhost", port + 2),
                Instance("localhost", port + 1),
                Instance("localhost", port),
            )
        )
        producer = OkHttpProducer(loadBalancer)
    }

    @Test
    fun stream() {

        val num = 4

        val messages = (0 until num)
            .map { _ -> "{ ${javaClass.simpleName}:${System.currentTimeMillis()} }" }

        val outbound = Flux.fromIterable(messages)
            .delayElements(Duration.ofMillis(100))
            .map { s -> s.toByteArray() }

        val topic = URI("ws:/default/ws?maxAttempts=3")

        val result = producer.request(topic, outbound)
            .take(num.toLong())

        val done = StepVerifier.create(result)
            .expectNextCount(num.toLong())
            .verifyComplete()

        assertTrue(done < Duration.ofSeconds(30))
    }

    @Test
    fun send() {

        val msg = "{ ${javaClass.simpleName} }".toByteArray()

        val topic = URI("ws:/default/ws?maxAttempts=3")

        val m = producer.send(topic, msg)

        val done = StepVerifier.create(m)
            .verifyComplete()
        //.verifyErrorMatches { e -> e is IllegalStateException }

        assertTrue(done < Duration.ofSeconds(10))
    }
}