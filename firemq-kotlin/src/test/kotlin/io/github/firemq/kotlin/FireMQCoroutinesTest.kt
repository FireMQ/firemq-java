package io.github.firemq.kotlin

import io.github.firemq.Client
import io.github.firemq.FireMQException
import io.github.firemq.PublishOptions
import io.github.firemq.QueueOptions
import io.github.firemq.SubscribeOptions
import io.github.firemq.UnroutableException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class FireMQCoroutinesTest {
    @Test
    fun `connect errors surface as FireMQException`(): Unit = runBlocking {
        val e = assertFailsWith<FireMQException> { connectAwait("ws://localhost:15672/fire") }
        assertTrue(e.message!!.contains("WebSocket"))
    }
}

/** Runs against a live broker when FIREMQ_URL is set, for example fire://guest:guest@localhost:5673. */
@EnabledIfEnvironmentVariable(named = "FIREMQ_URL", matches = ".+")
class FireMQCoroutinesIntegrationTest {
    private val url = System.getenv("FIREMQ_URL")

    private fun withClient(block: suspend (Client) -> Unit): Unit = runBlocking {
        val client = connectAwait(url)
        try {
            withTimeout(30_000) { block(client) }
        } finally {
            client.closeAwait()
        }
    }

    @Test
    fun `publish and consume with a flow`() = withClient { client ->
        // Not auto-delete: the queue must outlive the flow's subscription for the final check.
        val queue = client.declareQueueAwait("", QueueOptions().durable(false).exclusive(true)).name
        coroutineScope {
            (1..50).map { i -> async { client.publishAwait("", queue, "m$i") } }.awaitAll()
        }

        val bodies = client.subscribeFlow(queue, SubscribeOptions().prefetch(10))
            .take(50)
            .map { msg ->
                msg.ack()
                msg.text()
            }
            .toList()

        assertEquals((1..50).map { "m$it" }.toSet(), bodies.toSet())
        assertNull(client.getAwait(queue, noAck = true))
    }

    @Test
    fun `topology and get`() = withClient { client ->
        val exchange = "kotlin.test.topic"
        client.declareExchangeAwait(exchange, "topic")
        val queue = client.declareQueueAwait("", QueueOptions.temporary()).name
        client.bindQueueAwait(queue, exchange, "order.*")
        client.publishAwait(exchange, "order.created", "hello".toByteArray())
        val msg = client.getAwait(queue)!!
        assertEquals("hello", msg.text())
        assertEquals("order.created", msg.routingKey())
        msg.ack()
        assertEquals(0, client.purgeQueueAwait(queue))
        client.unbindQueueAwait(queue, exchange, "order.*")
        client.deleteExchangeAwait(exchange)
    }

    @Test
    fun `mandatory publish to nowhere throws UnroutableException`() = withClient { client ->
        assertFailsWith<UnroutableException> {
            client.publishAwait("", "kotlin.test.nowhere", "lost", PublishOptions().mandatory(true))
        }
    }
}
