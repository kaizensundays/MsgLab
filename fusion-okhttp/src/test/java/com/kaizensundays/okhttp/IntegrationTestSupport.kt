package com.kaizensundays.okhttp

import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig

/**
 * Created: Saturday 9/30/2023, 8:18 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
@SpringJUnitConfig
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [IntegrationTestSupport::class])
@EnableAutoConfiguration
open class IntegrationTestSupport : TestSupport() {

}