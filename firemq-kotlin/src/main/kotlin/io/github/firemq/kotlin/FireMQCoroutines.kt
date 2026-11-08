@file:JvmName("FireMQCoroutines")

package io.github.firemq.kotlin

import io.github.firemq.Client
import io.github.firemq.ConnectOptions
import io.github.firemq.ExchangeOptions
import io.github.firemq.FireMQ
import io.github.firemq.Message
import io.github.firemq.PublishOptions
import io.github.firemq.QueueInfo
import io.github.firemq.QueueOptions
import io.github.firemq.SubscribeOptions
import io.github.firemq.Subscription
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext

/*
 * Coroutine extensions for the FireMQ Java client.
 *
 * Each `...Await` function is the suspending form of the client method with the same name: it
 * suspends instead of blocking a thread and throws the same FireMQException subclasses.
 * Subscriptions are exposed as cold Flows with subscribeFlow.
 */

/** Connects with a URL such as `fire://user:pass@host:5673/vhost`, without blocking the caller. */
suspend fun connectAwait(url: String): Client = withContext(Dispatchers.IO) { FireMQ.connect(url) }

/** Connects with explicit options, without blocking the caller. */
suspend fun connectAwait(options: ConnectOptions): Client = withContext(Dispatchers.IO) { FireMQ.connect(options) }

/** Closes the connection gracefully, without blocking the caller. */
suspend fun Client.closeAwait() = withContext(Dispatchers.IO) { close() }

/** Declares a queue. Queues are durable by default; use [QueueOptions.temporary] for a private one. */
suspend fun Client.declareQueueAwait(name: String, options: QueueOptions = QueueOptions()): QueueInfo =
    declareQueueAsync(name, options).await()

/** Deletes a queue and returns how many messages it held. */
suspend fun Client.deleteQueueAwait(name: String, ifUnused: Boolean = false, ifEmpty: Boolean = false): Long =
    deleteQueueAsync(name, ifUnused, ifEmpty).await()

/** Removes every ready message from a queue and returns how many were removed. */
suspend fun Client.purgeQueueAwait(name: String): Long = purgeQueueAsync(name).await()

/** Declares an exchange of type `direct`, `fanout`, `topic` or `headers`. */
suspend fun Client.declareExchangeAwait(name: String, type: String, options: ExchangeOptions = ExchangeOptions()) {
    declareExchangeAsync(name, type, options).await()
}

suspend fun Client.deleteExchangeAwait(name: String, ifUnused: Boolean = false) {
    deleteExchangeAsync(name, ifUnused).await()
}

/** Binds [queue] to [exchange] with a routing key or pattern. */
suspend fun Client.bindQueueAwait(queue: String, exchange: String, key: String, args: Map<String, Any?>? = null) {
    bindQueueAsync(queue, exchange, key, args).await()
}

suspend fun Client.unbindQueueAwait(queue: String, exchange: String, key: String, args: Map<String, Any?>? = null) {
    unbindQueueAsync(queue, exchange, key, args).await()
}

/** Binds exchange [destination] to exchange [source]. */
suspend fun Client.bindExchangeAwait(destination: String, source: String, key: String, args: Map<String, Any?>? = null) {
    bindExchangeAsync(destination, source, key, args).await()
}

suspend fun Client.unbindExchangeAwait(destination: String, source: String, key: String, args: Map<String, Any?>? = null) {
    unbindExchangeAsync(destination, source, key, args).await()
}

/**
 * Publishes a message and suspends until the broker confirms it (unless confirms are turned off in
 * [options]). Throws [io.github.firemq.NackedException] or [io.github.firemq.UnroutableException]
 * when the broker rejects or returns the message.
 */
suspend fun Client.publishAwait(exchange: String, key: String, body: ByteArray, options: PublishOptions = PublishOptions()) {
    publishAsync(exchange, key, body, options).await()
}

/** Publishes a UTF-8 text message and suspends until the broker confirms it. */
suspend fun Client.publishAwait(exchange: String, key: String, body: String, options: PublishOptions = PublishOptions()) {
    publishAsync(exchange, key, body, options).await()
}

/** Takes one message from a queue, or returns null when it is empty. */
suspend fun Client.getAwait(queue: String, noAck: Boolean = false): Message? = getAsync(queue, noAck).await()

/** Starts consuming from a queue. Prefer [subscribeFlow] unless you need the [Subscription] itself. */
suspend fun Client.subscribeAwait(queue: String, options: SubscribeOptions = SubscribeOptions()): Subscription =
    subscribeAsync(queue, options).await()

/** Suspends until the next message arrives. Returns null once the subscription has ended. */
suspend fun Subscription.nextAwait(): Message? = nextAsync().await()

/** Stops the subscription and suspends until the broker confirms. */
suspend fun Subscription.cancelAwait() {
    cancelAsync().await()
}

/**
 * The messages of an existing subscription as a Flow. The flow completes when the subscription
 * ends. Collecting it does not cancel the subscription; see [subscribeFlow] for that.
 */
fun Subscription.asFlow(): Flow<Message> = flow {
    while (true) {
        val msg = nextAwait() ?: break
        emit(msg)
    }
}

/**
 * Subscribes to [queue] when collected and emits its messages in order. Acknowledge each message
 * with [Message.ack] unless [SubscribeOptions.noAck] is set; credit is topped up as you do.
 *
 * The flow completes when the broker cancels the subscription or the connection closes. When the
 * collector stops early (for example with `take(n)` or by being cancelled), the subscription is
 * cancelled; messages it already received but did not emit stay unacknowledged until the
 * connection closes.
 *
 * ```kotlin
 * client.subscribeFlow("jobs").collect { msg ->
 *     println(msg.text())
 *     msg.ack()
 * }
 * ```
 */
fun Client.subscribeFlow(queue: String, options: SubscribeOptions = SubscribeOptions()): Flow<Message> = flow {
    val sub = subscribeAwait(queue, options)
    try {
        while (true) {
            val msg = sub.nextAwait() ?: break
            emit(msg)
        }
    } finally {
        withContext(NonCancellable) {
            runCatching { sub.cancelAwait() }
        }
    }
}
