package com.kaizensundays.okhttp

/**
 * Created: Saturday 10/21/2023, 4:29 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
data class TopicOptions(
    var maxAttempts: Long = 1,
    var minBackoffSec: Long = 1,
    var maxBackoffSec: Long = 10,
    var timeoutSec: Long = 10
)
