package io.github.firemq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Runs against a live broker when FIREMQ_URL is set, for example fire://guest:guest@localhost:5673. */
@EnabledIfEnvironmentVariable(named = "FIREMQ_URL", matches = ".+")
@Timeout(60)
class ClientIntegrationTest {
    private static final Duration WAIT = Duration.ofSeconds(5);
    private Client client;
    private final List<String> exchanges = new ArrayList<>();

    @BeforeEach
    void connect() {
        client = FireMQ.connect(System.getenv("FIREMQ_URL"));
    }

    @AfterEach
    void close() {
        if (client != null && !client.isClosed()) {
            for (String x : exchanges) {
                try {
                    client.deleteExchange(x);
                } catch (FireMQException ignored) {
                    // Best effort.
                }
            }
            client.close();
        }
    }

    private String tempQueue() {
        return client.declareQueue("", QueueOptions.temporary()).name();
    }

    private String exchange(String type) {
        String name = "java-it." + type + "." + UUID.randomUUID();
        client.declareExchange(name, type, new ExchangeOptions().durable(false));
        exchanges.add(name);
        return name;
    }

    @Test
    void handshakeReportsTheServer() {
        assertEquals("firemq", client.server().name());
        assertTrue(client.server().capabilities().contains("confirms"));
        assertTrue(client.server().maxFrame() >= 65536);
    }

    @Test
    void publishGetAndAck() {
        String q = tempQueue();
        assertNull(client.get(q, true));
        PublishOptions o = new PublishOptions()
                .properties(new MessageProperties().contentType("text/plain").correlationId("c-1").priority(3)
                        .timestamp(1767225600L).expiration(600000L))
                .header("trace", "abc")
                .header("n", 5)
                .header("list", List.of(1, "two"));
        client.publish("", q, "hello", o);
        Message m = client.get(q, false);
        assertNotNull(m);
        assertEquals("hello", m.text());
        assertEquals("", m.exchange());
        assertEquals(q, m.routingKey());
        assertFalse(m.redelivered());
        assertEquals("text/plain", m.properties().contentType());
        assertEquals("c-1", m.properties().correlationId());
        assertEquals(3, m.properties().priority());
        assertEquals(1767225600L, m.properties().timestamp());
        assertEquals(600000L, m.properties().expiration());
        assertEquals("abc", m.headers().get("trace"));
        assertEquals(5L, m.headers().get("n"));
        assertEquals(List.of(1L, "two"), m.headers().get("list"));
        m.ack();
        assertNull(client.get(q, true));
    }

    /** Polls with get, for effects the broker applies asynchronously such as dead-lettering. */
    private Message getEventually(String queue) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (true) {
            Message m = client.get(queue, true);
            if (m != null || System.nanoTime() > deadline) {
                return m;
            }
            Thread.sleep(20);
        }
    }

    @Test
    void consumeManyWithASmallWindow() throws Exception {
        // Not auto-delete, so the queue survives the subscription for the final check.
        String q = client.declareQueue("", new QueueOptions().durable(false).exclusive(true)).name();
        List<CompletableFuture<Void>> confirms = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            confirms.add(client.publishAsync("", q, "m" + i));
        }
        CompletableFuture.allOf(confirms.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
        try (Subscription s = client.subscribe(q, new SubscribeOptions().prefetch(10))) {
            for (int i = 0; i < 500; i++) {
                Message m = s.next(WAIT);
                assertNotNull(m, "message " + i);
                assertEquals("m" + i, m.text());
                m.ack();
            }
        }
        assertEquals(0, client.declareQueue(q, new QueueOptions().passive(true)).messages());
    }

    @Test
    void noAckSubscriptionsKeepFlowing() {
        String q = tempQueue();
        try (Subscription s = client.subscribe(q, new SubscribeOptions().prefetch(4).noAck(true))) {
            for (int i = 0; i < 40; i++) {
                client.publish("", q, "n" + i);
            }
            int seen = 0;
            for (Message m : s) {
                assertEquals("n" + seen, m.text());
                if (++seen == 40) {
                    break;
                }
            }
            assertEquals(40, seen);
        }
    }

    @Test
    void prefetchLimitsDeliveries() {
        String q = tempQueue();
        for (int i = 0; i < 3; i++) {
            client.publish("", q, "p" + i);
        }
        try (Subscription s = client.subscribe(q, new SubscribeOptions().prefetch(2))) {
            Message a = s.next(WAIT);
            Message b = s.next(WAIT);
            assertNotNull(a);
            assertNotNull(b);
            assertNull(s.next(Duration.ofMillis(300)));
            a.ack();
            b.ack();
            assertEquals("p2", s.next(WAIT).text());
        }
    }

    @Test
    void callbackSubscriptions() throws Exception {
        String q = tempQueue();
        CountDownLatch done = new CountDownLatch(10);
        List<String> got = new CopyOnWriteArrayList<>();
        try (Subscription s = client.subscribe(q, new SubscribeOptions(), m -> {
            got.add(m.text());
            m.ack();
            done.countDown();
        })) {
            for (int i = 0; i < 10; i++) {
                client.publish("", q, "c" + i);
            }
            assertTrue(done.await(10, TimeUnit.SECONDS));
        }
        assertEquals(10, got.size());
        assertEquals("c0", got.get(0));
    }

    @Test
    void nackRequeueRedelivers() {
        String q = tempQueue();
        client.publish("", q, "again");
        try (Subscription s = client.subscribe(q, new SubscribeOptions().prefetch(1))) {
            Message first = s.next(WAIT);
            assertFalse(first.redelivered());
            first.nack(true);
            Message second = s.next(WAIT);
            assertEquals("again", second.text());
            assertTrue(second.redelivered());
            second.ack();
        }
    }

    @Test
    void nackWithoutRequeueDeadLetters() throws InterruptedException {
        String dlx = exchange("fanout");
        String dead = tempQueue();
        client.bindQueue(dead, dlx, "");
        String work = client.declareQueue("", QueueOptions.temporary().arg("x-dead-letter-exchange", dlx)).name();
        client.publish("", work, "bad");
        client.get(work, false).nack(false);
        Message m = getEventually(dead);
        assertNotNull(m);
        assertEquals("bad", m.text());
        assertEquals(dlx, m.exchange());
        assertTrue(m.headers().containsKey("x-death"));
    }

    @Test
    void mandatoryUnroutableAndOverflowNack() {
        UnroutableException u = assertThrows(UnroutableException.class,
                () -> client.publish("", "java-it.nowhere." + UUID.randomUUID(), "lost", new PublishOptions().mandatory(true)));
        assertEquals(312, u.code());
        client.publish("", "java-it.nowhere", "dropped"); // not mandatory: confirmed and dropped

        String q = client.declareQueue("", QueueOptions.temporary()
                .arg("x-max-length", 1).arg("x-overflow", "reject-publish")).name();
        client.publish("", q, "fits");
        NackedException n = assertThrows(NackedException.class, () -> client.publish("", q, "rejected"));
        assertEquals(406, n.code());
    }

    @Test
    void topicAndExchangeToExchangeRouting() {
        String topic = exchange("topic");
        String q = tempQueue();
        client.bindQueue(q, topic, "*.critical.#");
        for (String key : List.of("kern.critical", "kern.critical.disk", "kern.info", "critical")) {
            client.publish(topic, key, key);
        }
        assertEquals("kern.critical", client.get(q, true).text());
        assertEquals("kern.critical.disk", client.get(q, true).text());
        assertNull(client.get(q, true));

        String up = exchange("direct");
        String down = exchange("fanout");
        String q2 = tempQueue();
        client.bindQueue(q2, down, "");
        client.bindExchange(down, up, "audit");
        client.publish(up, "audit", "yes");
        client.publish(up, "other", "no");
        assertEquals("yes", client.get(q2, true).text());
        assertNull(client.get(q2, true));
        client.unbindExchange(down, up, "audit");
        client.publish(up, "audit", "unbound");
        assertNull(client.get(q2, true));
        client.unbindQueue(q, topic, "*.critical.#");
    }

    @Test
    void headersExchangeBindingArguments() {
        String hx = exchange("headers");
        String q = tempQueue();
        client.bindQueue(q, hx, "", Map.of("x-match", "all", "format", "pdf"));
        client.publish(hx, "", "pdf", new PublishOptions().header("format", "pdf"));
        client.publish(hx, "", "zip", new PublishOptions().header("format", "zip"));
        assertEquals("pdf", client.get(q, true).text());
        assertNull(client.get(q, true));
    }

    @Test
    void errorsLeaveTheConnectionUsable() {
        String name = "java-it.durable." + UUID.randomUUID();
        client.declareQueue(name);
        try {
            ServerException e = assertThrows(ServerException.class,
                    () -> client.declareQueue(name, new QueueOptions().durable(false)));
            assertEquals(406, e.code());
            ServerException missing = assertThrows(ServerException.class,
                    () -> client.bindQueue(name, "java-it.missing." + UUID.randomUUID(), ""));
            assertEquals(404, missing.code());
            client.publish("", name, "x");
            client.publish("", name, "y");
            assertEquals(2, client.purgeQueue(name));
            client.publish("", name, "z");
        } finally {
            assertEquals(1, client.deleteQueue(name));
        }
        assertEquals(404, assertThrows(ServerException.class,
                () -> client.declareQueue(name, new QueueOptions().passive(true))).code());
    }

    @Test
    void binaryAndLargeBodies() {
        String q = tempQueue();
        byte[] binary = new byte[256];
        for (int i = 0; i < binary.length; i++) {
            binary[i] = (byte) i;
        }
        byte[] large = "x".repeat(1 << 20).getBytes(StandardCharsets.UTF_8);
        client.publish("", q, binary);
        client.publish("", q, large);
        assertTrue(java.util.Arrays.equals(binary, client.get(q, true).body()));
        assertTrue(java.util.Arrays.equals(large, client.get(q, true).body()));
    }

    @Test
    void unackedMessagesReturnWhenTheConnectionCloses() {
        String name = "java-it.requeue." + UUID.randomUUID();
        client.declareQueue(name, new QueueOptions().durable(false));
        try {
            client.publish("", name, "work");
            try (Client other = FireMQ.connect(System.getenv("FIREMQ_URL"))) {
                Subscription s = other.subscribe(name, new SubscribeOptions().prefetch(1));
                assertEquals("work", s.next(WAIT).text());
            }
            Message m = client.get(name, true);
            assertNotNull(m);
            assertTrue(m.redelivered());
        } finally {
            client.deleteQueue(name);
        }
    }

    @Test
    void closingEndsSubscriptionsAndFailsCalls() {
        String q = tempQueue();
        Subscription s = client.subscribe(q);
        client.close();
        assertTrue(client.isClosed());
        assertNull(s.next(WAIT));
        assertFalse(s.isActive());
        assertThrows(ConnectionClosedException.class, () -> client.publish("", q, "late"));
    }

    @Test
    void shortHeartbeatsKeepTheConnectionAlive() throws Exception {
        try (Client c = FireMQ.connect(ConnectOptions.fromUrl(System.getenv("FIREMQ_URL")).heartbeat(1))) {
            assertEquals(1, c.server().heartbeat());
            Thread.sleep(3500);
            assertFalse(c.isClosed());
            String q = c.declareQueue("", QueueOptions.temporary()).name();
            c.publish("", q, "alive");
            assertEquals("alive", c.get(q, true).text());
        }
    }

    @Test
    void wrongPasswordIsRefused() {
        ConnectUrl u = ConnectUrl.parse(System.getenv("FIREMQ_URL"));
        ConnectUrl bad = new ConnectUrl(u.transport(), u.host(), u.port(), u.vhost(), u.user(), "wrong-password", u.path());
        ServerException e = assertThrows(ServerException.class, () -> FireMQ.connect(new ConnectOptions(bad)));
        assertEquals(403, e.code());
    }

    /** Needs FIREMQ_TLS_URL (fires://...) and FIREMQ_TLS_CA (path to the CA certificate in PEM). */
    @Test
    @EnabledIfEnvironmentVariable(named = "FIREMQ_TLS_URL", matches = ".+")
    void tlsWithAnExtraCa() throws Exception {
        byte[] ca = Files.readAllBytes(Path.of(System.getenv("FIREMQ_TLS_CA")));
        try (Client c = FireMQ.connect(ConnectOptions.fromUrl(System.getenv("FIREMQ_TLS_URL")).tlsCaPem(ca))) {
            String q = c.declareQueue("", QueueOptions.temporary()).name();
            c.publish("", q, "secure");
            assertEquals("secure", c.get(q, true).text());
        }
        // Without the CA the certificate is not trusted.
        assertThrows(FireMQException.class, () -> FireMQ.connect(System.getenv("FIREMQ_TLS_URL")).close());
        try (Client c = FireMQ.connect(ConnectOptions.fromUrl(System.getenv("FIREMQ_TLS_URL")).tlsInsecure(true))) {
            assertFalse(c.isClosed());
        }
    }
}
